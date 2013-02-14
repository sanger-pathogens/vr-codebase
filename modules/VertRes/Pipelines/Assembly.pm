=head1 NAME

VertRes::Pipelines::Assembly - Assemble genomes

=head1 SYNOPSIS

echo '__Assembly__ assembly.conf' > pipeline.config
# where assembly.conf contains:
root    => '/lustre/scratch108/pathogen/pathpipe/prokaryotes/seq-pipelines',
module  => 'VertRes::Pipelines::Assembly',
prefix  => '_',
limit   => 30,

limits => {
 project => ['ABC']
},
vrtrack_processed_flags => {  stored => 1, import => 1, assembled => 0 },

db  => {
    database => 'pathogen_prok_track',
    host     => 'xxx',
    port     => yyyy,
    user     => 'pathpipe_rw',
    password => 'xxx',
},

data => {
    db  => {
        database => 'pathogen_prok_track',
        host     => 'xxx',
        port     => yyyy,
        user     => 'pathpipe_rw',
        password => 'xxx',
    },
    # rough estimate so we know how much RAM to request
    genome_size => 10000000,

    seq_pipeline_root    => '/lustre/scratch108/pathogen/pathpipe/prokaryotes/seq-pipelines',
    no_scaffolding => 0,
    annotation     => 1,

    assembler => 'velvet',
    assembler_exec => '/software/pathogen/external/apps/usr/bin/velvet',
    optimiser_exec => '/software/pathogen/external/apps/usr/bin/VelvetOptimiser.pl',
    max_threads => 1,
},


=head1 DESCRIPTION

This pipeline requires velvet, prokka, smalt, SSPACE and GapFiller to be separately installed from the original authors software repositories.


=head1 AUTHOR

path-help@sanger.ac.uk

=cut
package VertRes::Pipelines::Assembly;

use strict;
use warnings;
use VertRes::Utils::Hierarchy;
use VertRes::IO;
use VertRes::Utils::FileSystem;
use VRTrack::VRTrack;
use VRTrack::Lane;
use VRTrack::Library;
use VRTrack::File;
use File::Basename;
use Time::Format;
use File::Copy;
use Cwd;
use LSF;
use Data::Dumper;
use FileHandle;
use VertRes::Utils::Assembly;
use Bio::AssemblyImprovement::Scaffold::Descaffold;
use Bio::AssemblyImprovement::Scaffold::SSpace::PreprocessInputFiles;
use Bio::AssemblyImprovement::Scaffold::SSpace::Iterative;
use Bio::AssemblyImprovement::FillGaps::GapFiller::Iterative;

use base qw(VertRes::Pipeline);

our $actions = [ { name     => 'pool_fastqs',
                   action   => \&pool_fastqs,
                   requires => \&pool_fastqs_requires,
                   provides => \&pool_fastqs_provides },
                 { name     => 'optimise_parameters',
                   action   => \&optimise_parameters,
                   requires => \&optimise_parameters_requires,
                   provides => \&optimise_parameters_provides },
                 { name     => 'map_back',
                   action   => \&map_back,
                   requires => \&map_back_requires, 
                   provides => \&map_back_provides },
                 { name     => 'update_db',
                   action   => \&update_db,
                   requires => \&update_db_requires, 
                   provides => \&update_db_provides },
                 { name     => 'cleanup',
                   action   => \&cleanup,
                   requires => \&cleanup_requires,
                   provides => \&cleanup_provides }
                ];

our %options = (
                do_cleanup      => 1,
                scaffolder_exec => '/software/pathogen/external/apps/usr/local/SSPACE-BASIC-2.0_linux-x86_64/SSPACE_Basic_v2.0.pl',
                gap_filler_exec => '/software/pathogen/external/apps/usr/local/GapFiller_v1-10_linux-x86_64/GapFiller.pl',
                abacas_exec     => '/software/pathogen/internal/prod/bin/abacas.pl',
                no_scaffolding  => 0,
                annotation      => 0,
                );

sub new {
  my ($class, @args) = @_;

  my $self = $class->SUPER::new(%options, actions => $actions, @args);
  if(defined($self->{db}))
  {
    $self->{vrtrack} = VRTrack::VRTrack->new($self->{db}) or $self->throw("Could not connect to the database\n");
  }
  $self->{fsu} = VertRes::Utils::FileSystem->new;

  unless(defined($self->{max_threads}))
  {
    $self->{max_threads} = 1;
  }
  
  if(defined($self->{vrlane}))
  {
    # being run on individual lanes
    $self->{pools} = 
       [{
            lanes => [$self->{vrlane}->name()],
            type => 'shortPaired',
       }];
  }

  my $assembly_util = VertRes::Utils::Assembly->new(assembler => $self->{assembler});
  my $assembler_class = $assembly_util->find_module();
  eval "require $assembler_class;";
  $self->{assembler_class} = $assembler_class;
  
  
  return $self;
}

###########################
# Begin map_back
###########################

sub map_back_requires
{
  my ($self) = @_;
  return $self->optimise_parameters_provides();
}

sub map_back_provides
{
   my ($self) = @_;

   return [$self->{lane_path}."/".$self->{prefix}.$self->{assembler}.'_map_back_done'];

}

sub map_back
{
  my ($self, $build_path, $action_lock) = @_;
  $self->mapping_and_generate_stats($build_path, $action_lock, "optimised_directory", "map_back",$self->{reference});
  
  return $self->{No};
}


###########################
# End map_back
###########################



sub mapping_and_generate_stats
{
  my ($self, $build_path, $action_lock, $working_directory_method_name, $action_name_suffix, $reference) = @_;
  
  my $assembler_class = $self->{assembler_class};
  my $output_directory = $self->{lane_path};
  eval("use $assembler_class; ");
  my $assembler_util= $assembler_class->new( output_directory => qq[$output_directory]);
  my $base_path = $self->{seq_pipeline_root};
  
  my $job_name = $self->{prefix}.$self->{assembler}."_$action_name_suffix";
  my $script_name = $self->{fsu}->catfile($output_directory, $self->{prefix}.$self->{assembler}."_$action_name_suffix.pl");
  
  my $lane_names = $self->get_all_lane_names($self->{pools});
  my @lane_paths;
  for my $lane_name (@$lane_names)
  {
    push(@lane_paths,$base_path.'/'.$self->{vrtrack}->hierarchy_path_of_lane_name($lane_name).'/'.$lane_name);
  }
  my $lane_paths_str = '("'.join('","', @lane_paths).'")';
  my $reference_str = '';
  if($reference)
  {
    $reference_str = qq{,reference => qq[$reference]} ;
  }
  my $annotation = $self->{annotation};

  open(my $scriptfh, '>', $script_name) or $self->throw("Couldn't write to temp script $script_name: $!");
  print $scriptfh qq{
  use strict;
  use $assembler_class;

  unlink("pool_1.fastq.gz");
    
  my \@lane_paths = $lane_paths_str;
  
  my \$assembler_util= $assembler_class->new( output_directory => qq[$output_directory] $reference_str );
  my \$directory = \$assembler_util->${working_directory_method_name}();
  \$assembler_util->map_and_generate_stats(\$directory,qq[$output_directory], \\\@lane_paths );
  
  unlink("\$directory/contigs.mapped.sorted.bam.bai");
  unlink("\$directory/contigs.mapped.sorted.bam");
  unlink("\$directory/contigs.fa.small.sma");
  unlink("\$directory/contigs.fa.small.smi");
  
  if($annotation == 1)
  {
    system("prokka --centre SC --cpus 2 --force --outdir  \$directory/annotation \$directory/contigs.fa");
  }
  system("touch \$directory/$self->{prefix}$self->{assembler}_${action_name_suffix}_done");
 
  system("touch $self->{prefix}$self->{assembler}_${action_name_suffix}_done");
  exit;
                };
  close $scriptfh;
  
  my $memory_required_mb = 1700;

  LSF::run($action_lock, $output_directory, $job_name, {bsub_opts => " -M${memory_required_mb}000 -R 'select[mem>$memory_required_mb] rusage[mem=$memory_required_mb]'"}, qq{perl -w $script_name});
  
}


###########################
# Begin optimise
###########################

sub optimise_parameters_provides
{
  my $self = shift;

  return  [$self->{lane_path}."/".$self->{prefix}."$self->{assembler}_optimise_parameters_done"];
}

sub optimise_parameters_requires
{
  my $self = shift;
  return $self->pool_fastqs_provides();
}

sub optimise_parameters
{
      my ($self, $build_path,$action_lock) = @_;

      my $lane_names = $self->get_all_lane_names($self->{pools});
      my $output_directory = $self->{lane_path};
      my $base_path = $self->{seq_pipeline_root};

      my $assembler_class = $self->{assembler_class};
      my $optimiser_exec = $self->{optimiser_exec};

      eval("use $assembler_class; ");
      my $assembler_util= $assembler_class->new();
      my $files_str = $assembler_util->generate_files_str($self->{pools}, $output_directory);

      my $job_name = $self->{prefix}.$self->{assembler}.'_optimise_parameters';
      my $script_name = $self->{fsu}->catfile($output_directory, $self->{prefix}.$self->{assembler}."_optimise_parameters.pl");
      
      my @lane_paths;
      for my $lane_name (@$lane_names)
      {
        push(@lane_paths,$base_path.'/'.$self->{vrtrack}->hierarchy_path_of_lane_name($lane_name).'/'.$lane_name);
      }
      my $lane_paths_str = '("'.join('","', @lane_paths).'")';

      my $kmer = $self->calculate_kmer_size();
      
      my $memory_required_mb = int($self->estimate_memory_required($output_directory, $kmer->{min})/1000);

      my $num_threads = $self->number_of_threads($memory_required_mb);
      my $insert_size = $self->get_insert_size();
      my $tmp_directory = $self->{tmp_directory}.'/'.$lane_names->[0] || getcwd();

      open(my $scriptfh, '>', $script_name) or $self->throw("Couldn't write to temp script $script_name: $!");
      print $scriptfh qq{
use strict;
use $assembler_class;
use VertRes::Pipelines::Assembly;
use File::Copy;
use Cwd;
use File::Path qw(make_path);
my \$assembly_pipeline = VertRes::Pipelines::Assembly->new();
system("rm -rf velvet_assembly_*");
make_path(qq[$tmp_directory]);
chdir(qq[$tmp_directory]);

my \$assembler = $assembler_class->new(
  assembler => qq[$self->{assembler}],
  optimiser_exec => qq[$optimiser_exec],
  min_kmer => $kmer->{min},
  max_kmer => $kmer->{max},
  files_str => qq[$files_str],
  output_directory => qq[$tmp_directory],
  );

my \$ok = \$assembler->optimise_parameters($num_threads);
my \@lane_paths = $lane_paths_str;
\$ok = \$assembler->split_reads(qq[$tmp_directory], \\\@lane_paths);
\$ok = \$assembly_pipeline->improve_assembly(\$assembler->optimised_directory().'/contigs.fa',[qq[$tmp_directory].'/forward.fastq',qq[$tmp_directory].'/reverse.fastq'],$insert_size);

move(qq[$tmp_directory].'/velvet_assembly_logfile.txt', qq[$output_directory].'/velvet_assembly_logfile.txt');
system("mv $tmp_directory/velvet_assembly $output_directory");

unlink(qq[$tmp_directory].'/forward.fastq');
unlink(qq[$tmp_directory].'/reverse.fastq');
unlink(qq[$tmp_directory].'/contigs.fa.scaffolded.filtered');

chdir(qq[$output_directory]);
unlink('pool_1.fastq.gz');
system('touch $self->{prefix}$self->{assembler}_optimise_parameters_done');
exit;
              };
              close $scriptfh;

      my $total_memory_mb = $num_threads*$memory_required_mb;
      if($total_memory_mb < 2000)
      {
        $total_memory_mb = 2000;
      }
      
      my $queue = $self->decide_appropriate_queue($memory_required_mb);
      
      LSF::run($action_lock, $output_directory, $job_name, {bsub_opts => "-n$num_threads -q $queue -M${total_memory_mb}000 -R 'select[mem>$total_memory_mb] rusage[mem=$total_memory_mb] span[hosts=1]'", dont_wait=>1}, qq{perl -w $script_name});

      # we've only submitted to LSF, so it won't have finished; we always return
      # that we didn't complete
      return $self->{No};
}

sub decide_appropriate_queue
{
  my ($self, $memory_required_mb) = @_;
  my $queue = 'long';
  if($memory_required_mb > 30000)
  {
    $queue = 'hugemem';
  }
  elsif($memory_required_mb < 3500)
  {
    $queue = 'normal';
  }
  return $queue;
}



sub improve_assembly
{
  my ($self,$assembly_file, $input_files, $insert_size) = @_;
  
  my $preprocess_input_files = Bio::AssemblyImprovement::Scaffold::SSpace::PreprocessInputFiles->new(
      input_files    => $input_files,
      input_assembly => $assembly_file,
      reference      => $self->{reference},
  );
  my $process_input_files_tmp_dir_obj = $preprocess_input_files->_temp_directory_obj();

  # scaffold and extend contigs
  my $scaffolding_obj = Bio::AssemblyImprovement::Scaffold::SSpace::Iterative->new(
      input_files     => $preprocess_input_files->processed_input_files,
      input_assembly  => $preprocess_input_files->processed_input_assembly,
      insert_size     => $insert_size,
      scaffolder_exec => $self->{scaffolder_exec}
  );
  $scaffolding_obj->run();

  my $scaffolding_output = $scaffolding_obj->final_output_filename;
  
  # order contigs on an assembly
  if(defined($self->{reference}) && (-e $self->{reference}))
  {
    $scaffolding_obj = Bio::AssemblyImprovement::Abacas::Iterative->new(
      reference      => $preprocess_input_files->processed_reference,
      input_assembly => $scaffolding_output,
      abacas_exec    => $self->{abacas_exec},
    );
    $scaffolding_obj->run();
  }

  # fill gaps
  my $fill_gaps_obj = Bio::AssemblyImprovement::FillGaps::GapFiller::Iterative->new(
      input_files     => $preprocess_input_files->processed_input_files,
      input_assembly  => $scaffolding_obj->final_output_filename,
      insert_size     => $insert_size,
      gap_filler_exec => $self->{gap_filler_exec},
      _output_prefix  => 'gapfilled'
  )->run();
  move($fill_gaps_obj->final_output_filename,$assembly_file);
  
  # descaffold if needed
  if(defined($self->{no_scaffolding}) && $self->{no_scaffolding} == 1)
  {
    my $descaffold_obj = Bio::AssemblyImprovement::Scaffold::Descaffold->new(input_assembly => $assembly_file);
    $descaffold_obj->run();
    move($descaffold_obj->output_filename,$assembly_file);
  }
}

# Get the requested insert size of the first lane. Not suitable for mixed insert sizes, should be run with standalone scripts in that case.
sub get_insert_size
{
  my ($self) = @_;
  my $lane_names = $self->get_all_lane_names($self->{pools});
  my $insert_size = 350;

  for my $lane_name (@{$lane_names})
  {
    my $vrlane  = VRTrack::Lane->new_by_name($self->{vrtrack}, $lane_name) or $self->throw("No such lane in the DB: [".$lane_name."]");
    eval
    {
      $insert_size = VRTrack::Library->new($self->{vrtrack}, $vrlane->library_id())->insert_size() ;
    };
  }
  return $insert_size;
}


sub lane_read_length
{
  my ($self) = @_;
  my $lane_names = $self->get_all_lane_names($self->{pools});
  my $vrlane  = VRTrack::Lane->new_by_name($self->{vrtrack}, @$lane_names[0]) or $self->throw("No such lane in the DB: [".@$lane_names[0]."]");
  my $read_length = $vrlane->read_len() || 75;
  return $read_length;
}

# check kmers between 66% and 90% of read size. Choose lane and find out read size
sub calculate_kmer_size
{
  my ($self) = @_;
  my %kmer_size;
  my $read_length = $self->lane_read_length();

  $kmer_size{min} = int($read_length*0.66);
  $kmer_size{max} = int($read_length*0.90);
  
  if($kmer_size{min} % 2 == 0)
  {
    $kmer_size{min}--;
  }
  
  if($kmer_size{max} % 2 == 0)
  {
    $kmer_size{max}--;
  }

  return \%kmer_size;
}

sub number_of_threads
{
  my ($self, $memory_required_mb) = @_;
  my $normal_queue_mem_limit = 35000;
  my $num_threads = 1;
  
  if($normal_queue_mem_limit/$memory_required_mb > 2)
  {
    $num_threads = int($normal_queue_mem_limit/$memory_required_mb);
  }
  if($num_threads  > $self->{max_threads})
  {
    $num_threads = $self->{max_threads};
  }
  
  return $num_threads;
}

###########################
# End optimise
###########################




###########################
# Begin pool fastqs
###########################


sub pool_fastqs
{
      my ($self, $build_path,$action_lock) = @_;

      my $lane_names = $self->get_all_lane_names($self->{pools});
      my $output_directory = $self->{lane_path};
      my $base_path =  $self->{seq_pipeline_root};

      my $script_name = $self->{fsu}->catfile($build_path, $self->{prefix}."pool_fastqs.pl");
      open(my $scriptfh, '>', $script_name) or $self->throw("Couldn't write to temp script $script_name: $!");
      print $scriptfh qq{
use strict;
use VertRes::Pipelines::Assembly;
my \$assembly= VertRes::Pipelines::Assembly->new();
my \@lane_names;
};

   for my $lane_name ( @$lane_names)
   {
     my $lane_path = $self->{vrtrack}->hierarchy_path_of_lane_name($lane_name);
     my $vlane = VRTrack::Lane->new_by_name($self->{vrtrack},$lane_name);
     my $file_names_str ;
     if( @{$vlane->files} == 2)
     {
       my @file_names ;
       for my $file_name (@{$vlane->files})
       {
         push(@file_names, $file_name->name );
       }
       $file_names_str = '("'.join('","',@file_names ).'")';
     }

     print $scriptfh qq{
       my \@filenames_array = $file_names_str;
       \$assembly->shuffle_sequences_fastq_gz("$lane_name", "$base_path/$lane_path", "$output_directory",\\\@filenames_array);
     };
   }

   my $pool_count = 1;
   for my $lane_pool (@{$self->{pools}})
   {
    my $lane_names_str = '("'.join('.fastq.gz","',@{$lane_pool->{lanes}}).'.fastq.gz")';
    print $scriptfh qq{
\@lane_names = $lane_names_str;
\$assembly->concat_fastq_gz_files(\\\@lane_names, "pool_$pool_count.fastq.gz", "$output_directory", "$output_directory");
};
     for my $lane_name (  @{$lane_pool->{lanes}}  )
     {
        print $scriptfh qq{
unlink("$output_directory/$lane_name.fastq.gz");
};
     }

     $pool_count++;
   }

   print $scriptfh qq{
system("touch $output_directory/$self->{prefix}pool_fastqs_done");
exit;
      };
      close $scriptfh;
      my $job_name = $self->{prefix}.'pool_fastqs';

      LSF::run($action_lock, $output_directory, $job_name, {bsub_opts => '-M80000 -R \'select[mem>80] rusage[mem=80]\''}, qq{perl -w $script_name});

      # we've only submitted to LSF, so it won't have finished; we always return
      # that we didn't complete
      return $self->{No};
}

sub pool_fastqs_requires
{
  [];
}

sub pool_fastqs_provides
{
   my $self = shift;
   my @provided_files ;
   push(@provided_files, $self->{lane_path}.'/'.$self->{prefix}."pool_fastqs_done");


   return \@provided_files;
}

sub get_all_lane_names
{
  my ($self, $pooled_lanes) = @_;
  my @all_lane_names ;

  for my $lane_pool (@$pooled_lanes)
  {
    for my $lane_name (@{$lane_pool->{lanes}})
    {
      push(@all_lane_names, $lane_name);
    }
  }
  return \@all_lane_names;
}

sub concat_fastq_gz_files
{
  my ($self, $filenames, $outputname, $input_directory, $output_directory) = @_;

  my $filenames_with_paths = '';

  if(@{$filenames} == 1)
  {
    move($input_directory.'/'.$filenames->[0], "$output_directory/$outputname");
  }
  else
  {
    for my $filename (@$filenames)
    {
      $filenames_with_paths = $filenames_with_paths.$input_directory.'/'.$filename.' ';
    }
    `gzip -cd $filenames_with_paths | gzip > $output_directory/$outputname`;
  }
}


# adapted from https://github.com/dzerbino/velvet/blob/master/shuffleSequences_fastq.pl
sub shuffle_sequences_fastq_gz
{
  my ($self, $name, $input_directory, $output_directory, $file_names) = @_;

  my $filenameA = $file_names->[0];
  my $filenameB = $file_names->[1];
  my $filenameOut = $name.".fastq.gz";

  open( my $FILEA, "-|",'gunzip -c '.$input_directory.'/'.$filenameA) ;
  open( my $FILEB, "-|",'gunzip -c '.$input_directory.'/'.$filenameB) ;
  open( my $OUTFILE, "|-", "gzip -c  > $output_directory".'/'."$filenameOut");

  # FIXME: if FileB contains more reads than fileA they will get missed! This is also a bug in Velvets shuffleSequences_fastq.pl
  while(<$FILEA>) {
    print $OUTFILE $_;
    $_ = <$FILEA>;
    print $OUTFILE $_;
    $_ = <$FILEA>;
    print $OUTFILE $_;
    $_ = <$FILEA>;
    print $OUTFILE $_;

    my $file_b_line = <$FILEB>;
    next unless(defined($file_b_line));
    print $OUTFILE $file_b_line;
    $_ = <$FILEB>;
    print $OUTFILE $_;
    $_ = <$FILEB>;
    print $OUTFILE $_;
    $_ = <$FILEB>;
    print $OUTFILE $_;
  }
  close($FILEA);
  close($FILEB);
  close($OUTFILE);
}

###########################
# End pool fastqs
###########################



sub total_number_of_reads
{
  my ($self) = @_;
  my $lane_names = $self->get_all_lane_names($self->{pools});
  my $total_reads = 0;

  for my $lane_name (@{$lane_names})
  {
    my $vrlane  = VRTrack::Lane->new_by_name($self->{vrtrack}, $lane_name) or $self->throw("No such lane in the DB: [".$lane_name."]");
    $total_reads += $vrlane->raw_reads();
  }
  return $total_reads;
}


#Gives the answer in kb.
sub estimate_memory_required
{
  my ($self, $output_directory, $kmer_size) = @_;

  my %memory_params ;
  $memory_params{total_number_of_reads} = $self->total_number_of_reads();
  $memory_params{genome_size}           = $self->{genome_size};
  $memory_params{read_length}           = $self->lane_read_length();
  $memory_params{kmer_size}             = $kmer_size;

  my $assembler_class = $self->{assembler_class};
  eval("use $assembler_class; ");
  my $assembler_util= $assembler_class->new(output_directory => $output_directory);
  my $memory_required_in_kb = $assembler_util->estimate_memory_required(\%memory_params);
  return $memory_required_in_kb;
}


=head2 cleanup_requires

 Title   : cleanup_requires
 Usage   : my $required_files = $obj->cleanup_requires('/path/to/lane');
 Function: Find out what files the cleanup action needs before it will run.
 Returns : array ref of file names
 Args    : lane path

=cut

sub cleanup_requires {
  my ($self) = @_;
  return [ $self->{prefix}."assembly_update_db_done"];
}

=head2 cleanup_provides

 Title   : cleanup_provides
 Usage   : my $provided_files = $obj->cleanup_provides('/path/to/lane');
 Function: Find out what files the cleanup action generates on success.
 Returns : array ref of file names
 Args    : lane path

=cut

sub cleanup_provides {
    my ($self) = @_;
    return [$self->{prefix}."assembly_cleanup_done"];
}

=head2 cleanup

 Title   : cleanup
 Usage   : $obj->cleanup('/path/to/lane', 'lock_filename');
 Function: Unlink all the pipeline-related files (_*) in a lane, as well
           as the split directory.
 Returns : $VertRes::Pipeline::Yes
 Args    : lane path, name of lock file to use

=cut

sub cleanup {
  my ($self, $lane_path, $action_lock) = @_;
#  return $self->{Yes} unless $self->{do_cleanup};
  
  my $prefix = $self->{prefix};
  
  # remove job files
  foreach my $file (qw(pool_fastqs 
    velvet_optimise_parameters 
    velvet_map_back )) 
    {
      foreach my $suffix (qw(o e pl)) 
      {
        unlink($self->{fsu}->catfile($lane_path, $prefix.$file.'.'.$suffix));
      }
  }
  
  # remove files
  foreach my $file (qw(contigs.fa.scaffolded.filtered .RData contigs.fa.png.Rout scaffolded.summaryfile.txt reverse.fastq forward.fastq)) 
  {
    unlink($self->{fsu}->catfile($lane_path, $file));
  }
  Utils::CMD("touch ".$self->{fsu}->catfile($lane_path,"$self->{prefix}assembly_cleanup_done")   );  
  
  return $self->{Yes};
}



=head2 update_db_requires

 Title   : update_db_requires
 Usage   : my $required_files = $obj->update_db_requires('/path/to/lane');
 Function: Find out what files the update_db action needs before it will run.
 Returns : array ref of file names
 Args    : lane path

=cut

sub update_db_requires {
    my ($self, $lane_path) = @_;
    return $self->map_back_provides();
}

=head2 update_db_provides

 Title   : update_db_provides
 Usage   : my $provided_files = $obj->update_db_provides('/path/to/lane');
 Function: Find out what files the update_db action generates on success.
 Returns : array ref of file names
 Args    : lane path

=cut

sub update_db_provides {
   my ($self) = @_;
    return [ $self->{lane_path}."/".$self->{prefix}."assembly_update_db_done"];
}

=head2 update_db

 Title   : update_db
 Usage   : $obj->update_db('/path/to/lane', 'lock_filename');
 Function: Records in the database that the lane has been improved.
 Returns : $VertRes::Pipeline::Yes or No, depending on if the action completed.
 Args    : lane path, name of lock file to use

=cut
sub update_db {
    my ($self, $lane_path, $action_lock) = @_;

    # all the done files are there, so just need to update the processed
    # flag in the database (if not already updated)
    my $vrlane = $self->{vrlane};
    return $$self{'Yes'} unless(defined($vrlane));
    
    my $vrtrack = $vrlane->vrtrack;
    
    return $$self{'Yes'} if $vrlane->is_processed('assembled');

    unless($vrlane->is_processed('assembled')){
      $vrtrack->transaction_start();
      $vrlane->is_processed('assembled',1);
      $vrlane->update() || $self->throw("Unable to set assembled status on lane $lane_path");
      $vrtrack->transaction_commit();
    }

    
    my $job_status =  File::Spec->catfile($lane_path, $self->{prefix} . 'job_status');
    Utils::CMD("rm $job_status") if (-e $job_status);
    Utils::CMD("touch ".$self->{fsu}->catfile($lane_path,"$self->{prefix}assembly_update_db_done")   );  

    return $$self{'Yes'};
}


