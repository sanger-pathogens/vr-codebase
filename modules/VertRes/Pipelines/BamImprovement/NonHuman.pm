=head1 NAME

VertRes::Pipelines::BamImprovement::NonHuman - pipeline for improving bam files prior to calling in non-human data

=head1 SYNOPSIS

# make the config files, which specifies the details for connecting to the
# VRTrack g1k-meta database and the data roots:
echo '__VRTrack_BamImprovement_NonHuman__ bamimprovement.conf' > bamimprovement.pipeline

# where bamimprovement.conf contains:
root    => '/abs/path/to/root/data/dir',
module  => 'VertRes::Pipelines::BamImprovement::NonHuman',
prefix  => '_',

db =>
   {
        database => 'pathogen_fy2_test',
        host     => 'mcs6',
        port     => 3346,
        user     => 'pathpipe_rw',
        password => 'xxxxxx',
   },

limits => {
     project => ['Streptococcus pneumoniae global lineages'],
     lane => ['7444_8#17'],               
},

data => 
     { 
          slx_mapper => 'smalt',
          reference => '/lustre/scratch108/pathogen/pathpipe/refs/Streptococcus/pneumoniae_Taiwan19F-14/Streptococcus_pneumoniae_Taiwan19F-14_v1.fa',
          assembly_name => 'Streptococcus_pneumoniae_Taiwan19F-14_v1',
          
          #default behaviour is NOT to keep input bam files. If you want to keep:
          keep_original_bam_files => 1,
     },

=head1 DESCRIPTION

This is a subclass of the VertRes::Pipelines::BamImprovement. We have written this subclass
in order to adjust the BamImprovement pipeline for non-human datasets. A few functions in the parent
class have been taken over completely, while others are either overriden or omitted all together.

IMPORTANT: The recalibrate task in this subclass DOES NOT carry out any actual BAM recalibration. It just takes 
the output of its predecessor task ("sort") presents this as its own output.

=head1 AUTHORs

path-help@sanger.ac.uk

=cut

package VertRes::Pipelines::BamImprovement::NonHuman;

use strict;
use warnings;

use base qw(VertRes::Pipelines::BamImprovement);
use VertRes::Utils::Hierarchy;
use VertRes::IO;
use VertRes::Utils::FileSystem;
use VertRes::Utils::Sam;
use VRTrack::VRTrack;
use VRTrack::Lane;
use VRTrack::File;
use File::Basename;
use File::Copy;
use Time::Format;
use LSF;



our @actions = ( 
                 #Override parent methods (except for "provides")
                 { name     => 'realign',
                   action   => \&realign,
                   requires => \&realign_requires, 
                   provides => \&VertRes::Pipelines::BamImprovement::realign_provides },
                   
                 #Inherit parent methods
                 { name     => 'sort',
                   action   => \&VertRes::Pipelines::BamImprovement::sort,
                   requires => \&VertRes::Pipelines::BamImprovement::sort_requires, 
                   provides => \&VertRes::Pipelines::BamImprovement::sort_provides },
                   
                 #Override parent methods (IMPORTANT: No actual recalibration here! See the DESCRIPTION pod) 
                 { name     => 'recalibrate',
                   action   => \&recalibrate,
                   requires => \&recalibrate_requires, 
                   provides => \&VertRes::Pipelines::BamImprovement::recalibrate_provides },                   

                 #Override parent's calmd                                            
                 { name     => 'calmd',
                   action   => \&calmd,
                   requires => \&VertRes::Pipelines::BamImprovement::calmd_requires, 
                   provides => \&VertRes::Pipelines::BamImprovement::calmd_provides },
                   
                  #Inherit parent methods for all of the tasks below:         
                 { name     => 'rewrite_header',
                   action   => \&VertRes::Pipelines::BamImprovement::rewrite_header,
                   requires => \&VertRes::Pipelines::BamImprovement::rewrite_header_requires, 
                   provides => \&VertRes::Pipelines::BamImprovement::rewrite_header_provides },
                 { name     => 'statistics',
                   action   => \&VertRes::Pipelines::BamImprovement::statistics,
                   requires => \&VertRes::Pipelines::BamImprovement::statistics_requires, 
                   provides => \&VertRes::Pipelines::BamImprovement::statistics_provides },
                 { name     => 'extract_intervals',
                   action   => \&VertRes::Pipelines::BamImprovement::extract_intervals,
                   requires => \&VertRes::Pipelines::BamImprovement::extract_intervals_requires, 
                   provides => \&VertRes::Pipelines::BamImprovement::extract_intervals_provides },
                 { name     => 'index',
                   action   => \&VertRes::Pipelines::BamImprovement::index,
                   requires => \&VertRes::Pipelines::BamImprovement::index_requires, 
                   provides => \&VertRes::Pipelines::BamImprovement::index_provides },
                 { name     => 'update_db',
                   action   => \&VertRes::Pipelines::BamImprovement::update_db,
                   requires => \&VertRes::Pipelines::BamImprovement::update_db_requires, 
                   provides => \&VertRes::Pipelines::BamImprovement::update_db_provides },
                 { name     => 'cleanup',
                   action   => \&VertRes::Pipelines::BamImprovement::cleanup,
                   requires => \&VertRes::Pipelines::BamImprovement::cleanup_requires, 
                   provides => \&VertRes::Pipelines::BamImprovement::cleanup_provides } );

our %options = (slx_mapper => 'bwa',
                '454_mapper' => 'ssaha',
                slx_mapper_alias => [],
                '454_mapper_alias' => [],
                snp_sites => '',
                calmde => 0,
                do_index => 0,
                do_cleanup => 1);

=head2 new

 Title   : new
 Usage   : my $obj = VertRes::Pipelines::BamImprovement::NonHuman->new(lane => '/path/to/lane');
 Function: Create a new VertRes::Pipelines::BamImprovement::NonHuman object;
 Returns : VertRes::Pipelines::BamImprovement::NonHuman object
 Args    : 
           reference => '/path/to/ref.fa' (no default, either this or the
                        male_reference and female_reference pair of args must be
                        supplied)
           assembly_name => 'NCBI37' (no default, must be set to the name of
                                      the reference)                                      
           slx_mapper => 'bwa'|'maq' (default bwa; the mapper you used for
                                      mapping SLX lanes)
           slx_mapper_alias => ['bwa','bwa_aln'] (optional; alternative names 
                         that may have been used for the slx_mapper specified)
           454_mapper => 'ssaha', (default ssaha; the mapper you used for
                                     mapping 454 lanes)
           454_mapper_alias => ['ssaha', 'ssaha_1.3'], (optional; alternative names 
                         that may have been used for the 454_mapper specified)

=cut

sub new {
    my ($class, @args) = @_;
    
    my $self = $class->SUPER::plain_new(%options, actions => \@actions, @args);
    
    # we should have been supplied the option 'lane_path' which tells us which
    # lane we're in, which lets us choose which mapper module to use.
    my $lane = $self->{lane} || $self->throw("lane readgroup not supplied, can't continue");
    my $lane_path = $self->{lane_path} || $self->throw("lane path not supplied, can't continue");
    
    # if we've been supplied a list of lane paths to work with, instead of
    # getting the lanes from the db, we won't have a vrlane object; make one
    if (! $self->{vrlane}) {
        $self->throw("db option was not supplied in config") unless $self->{db};
        my $vrtrack = VRTrack::VRTrack->new($self->{db}) or $self->throw("Could not connect to the database\n");
        my $vrlane  = VRTrack::Lane->new_by_name($vrtrack, $lane) or $self->throw("No such lane in the DB: [$lane]");
        $self->{vrlane} = $vrlane;
    }
    $self->{vrlane} || $self->throw("vrlane object missing");
    

    $self->throw("assembly_name must be supplied in conf") unless $self->{assembly_name};
    
    # get a list of bams in this lane we want to improve
    my $hu = VertRes::Utils::Hierarchy->new(verbose => $self->verbose);
    $self->{assembly_name} || $self->throw("no assembly_name!");
    my @bams = $hu->lane_bams($lane_path, vrtrack => $self->{vrlane}->vrtrack,
                                          assembly_name => $self->{assembly_name},
                                          slx_mapper => $self->{slx_mapper},
                                          '454_mapper' => $self->{'454_mapper'},
                                          slx_mapper_alias => $self->{slx_mapper_alias},
                                          '454_mapper_alias' => $self->{'454_mapper_alias'});
    @bams || $self->throw("no bams to improve in lane $lane_path!");
    $self->{in_bams} = \@bams;
    
    # set some stuff from the $hu we'll need in some actions later
    $self->{mapper_class} = $hu->{mapper_class};
    $self->{mapper_obj} = $hu->{mapper_obj};
    $self->{mapstats_obj} = $hu->{mapstats_obj};
    $self->{release_date} = "$time{'yyyymmdd'}"; 
    
    $self->{io} = VertRes::IO->new;
    $self->{fsu} = VertRes::Utils::FileSystem->new;
    
    $self->{header_changes}->{RG}->{rgid_fix} ||= 0;
    $self->{header_changes}->{RG}->{match_db} ||= 0;
    $self->{header_changes}->{SQ}->{remove_unique} ||= 0;
    $self->{header_changes}->{SQ}->{from_dict} ||= '';
    
    return $self;
}

=head2 realign_requires

 Title   : realign_requires
 Usage   : my $required_files = $obj->realign_requires('/path/to/lane');
 Function: Find out what files the realign action needs before it will run.
 Returns : array ref of file names
 Args    : lane path

=cut

sub realign_requires {
    my $self = shift;
    return [] if ($self->{extract_intervals_only});
    
    return [ @{$self->{in_bams}}, $self->{reference} ];
    
}

=head2 realign

 Title   : realign
 Usage   : $obj->realign('/path/to/lane', 'lock_filename');
 Function: realign bam files around known indels.
 Returns : $VertRes::Pipeline::Yes or No, depending on if the action completed.
 Args    : lane path, name of lock file to use

=cut

sub realign {
    my ($self, $lane_path, $action_lock) = @_;
    
    my $memory = $self->{memory};
    my $java_mem;
    if (! defined $memory || $memory < 3999) {
        $memory = 3999;
        $java_mem = 3800;
    }
    $java_mem ||= int($memory * 0.9);
    my $queue = $memory >= 16000 ? "hugemem" : "normal";
    
    my $orig_bsub_opts = $self->{bsub_opts};
    $self->{bsub_opts} = "-q $queue -M${memory}000 -R 'select[mem>$memory] rusage[mem=$memory]'";
    my $verbose = $self->verbose;
    
    my $tmp_dir = $self->{tmp_dir} || '';
    $tmp_dir = ", tmp_dir => q[$tmp_dir]" if $tmp_dir;
    
    
    foreach my $in_bam (@{$self->{in_bams}}) {
        my $base = basename($in_bam);
        my ($rel_bam) = $self->_bam_name_conversion($in_bam);
        
        next if -s $rel_bam;
        
        my $working_bam = $rel_bam;
        $working_bam =~ s/\.bam$/.working.bam/;
        my $done_file = $self->{fsu}->catfile($lane_path, '.realign_complete_'.$base);
        my $intervals_file = $in_bam . ".realignment.target.intervals";
        
        # run realign in an LSF call to a temp script
        my $script_name = $self->{fsu}->catfile($lane_path, $self->{prefix}."realign_$base.pl");
        
        open(my $scriptfh, '>', $script_name) or $self->throw("Couldn't write to temp script $script_name: $!");
        print $scriptfh qq{
use strict;
use VertRes::Wrapper::GATK;
use VertRes::Utils::Sam;
use File::Copy;

my \$in_bam = '$in_bam';
my \$rel_bam = '$rel_bam';
my \$working_bam = '$working_bam';
my \$done_file = '$done_file';
my \$intervals_file = '$intervals_file';

my \$gatk = VertRes::Wrapper::GATK->new(verbose => $verbose,
                                        java_memory => $java_mem,
                                        reference => '$self->{reference}',
                                        build => 'NONHUMAN');

\$gatk->realignment_targets('$in_bam', '$intervals_file');

# do the realignment, generating an uncompressed, name-sorted bam
unless (-s \$rel_bam) {
    \$gatk->indel_realigner_nonhuman(\$in_bam, \$intervals_file, \$working_bam, bam_compression => 0);
}

# check for truncation
if (-s \$working_bam) {
    my \$su = VertRes::Utils::Sam->new;
    my \$orig_records = \$su->num_bam_records(\$in_bam);
    my \$new_records = \$su->num_bam_records(\$working_bam);
    
    if (\$orig_records == \$new_records && \$new_records > 10) {
        move(\$working_bam, \$rel_bam) || die "Could not rename \$working_bam to \$rel_bam\n";
        
        # mark that we've completed
        open(my \$dfh, '>', \$done_file) || die "Could not write to \$done_file";
        print \$dfh "done\\n";
        close(\$dfh);
    }
    else {
        move(\$working_bam, "\$rel_bam.bad");
        die "\$working_bam is bad (\$new_records records vs \$orig_records), renaming to \$rel_bam.bad";
    }
}

exit;
        };
        close $scriptfh;
        
        my $job_name = $self->{prefix}.'realign_'.$base;
        $self->archive_bsub_files($lane_path, $job_name);
        
        LSF::run($action_lock, $lane_path, $job_name, $self, qq{perl -w $script_name});
    }
    
    # we've only submitted to LSF, so it won't have finished; we always return
    # that we didn't complete
    return $self->{No};
}

=head2 recalibrate_requires

 Title   : recalibrate_requires
 Usage   : my $required_files = $obj->recalibrate_requires('/path/to/lane');
 Function: DOES NOT RECALIBRATE. Added to this class to make subclassing BamImprovement less painful
 Returns : array ref of file names
 Args    : lane path

=cut

sub recalibrate_requires {
    my ($self, $lane_path) = @_;
    return [] if ($self->{extract_intervals_only});
      
    # we need bams
    my @requires;
    foreach my $in_bam (@{$self->{in_bams}}) {
        my (undef, $sort_bam, $recal_bam) = $self->_bam_name_conversion($in_bam);
        next if -s $recal_bam;
        push(@requires, $sort_bam);
    }
    
    return \@requires;
}

=head2 recalibrate

 Title   : recalibrate
 Usage   : $obj->recalibrate('/path/to/lane', 'lock_filename');
 Function: DOES NOT actually recalibrate (see the DESCRIPTION pod on top of the file). 
           Here we present the output of the previous task as the output of this task.
 Returns : $VertRes::Pipeline::Yes or No, depending on if the action completed.
 Args    : lane path, name of lock file to use

=cut

sub recalibrate {
    my ($self, $lane_path, $action_lock) = @_;

    foreach my $bam (@{$self->{in_bams}}) {
        my $base = basename($bam);
        my (undef, $in_bam, $recal_bam) = $self->_bam_name_conversion($bam);
        
        next if -s $recal_bam;
        my $done_file = $self->{fsu}->catfile($lane_path, '.recalibrate_complete_'.$base);
        my $script_name = $self->{fsu}->catfile($lane_path, $self->{prefix}."recalibrate_$base.pl");
        
        open(my $scriptfh, '>', $script_name) or $self->throw("Couldn't write to temp script $script_name: $!");
        print $scriptfh qq{
use strict;
use Utils;

my \$in_bam = '$in_bam';
my \$recal_bam = '$recal_bam';
my \$done_file = '$done_file';

#Copy the sorted bam as the fake output of a recalibrated BAM (keeps other tasks happy).
Utils::CMD("mv $in_bam $recal_bam");
    
# mark that we've "completed"
open(my \$dfh, '>', \$done_file) || die "Could not write to \$done_file";
print \$dfh "done\\n";
close(\$dfh);
exit;
        };
        close $scriptfh;
        
        my $job_name = $self->{prefix}.'recalibrate_'.$base;
        $self->archive_bsub_files($lane_path, $job_name);
        
        LSF::run($action_lock, $lane_path, $job_name, $self, qq{perl -w $script_name});
    }
    
    return $self->{No};

}


=head2 calmd

 Title   : calmd
 Usage   : $obj->calmd('/path/to/lane', 'lock_filename');
 Function: We extend parents's calmd a bit here. Specifically, We have added a bit code 
           to make sure that the BAM resulting from calmd is indexed right away.
 Returns : $VertRes::Pipeline::Yes or No, depending on if the action completed.
 Args    : lane path, name of lock file to use

=cut

sub calmd {
    my ($self, $lane_path, $action_lock) = @_;
    
    my $orig_bsub_opts = $self->{bsub_opts};
    $self->{bsub_opts} = '-q normal -M1000000 -R \'select[mem>1000] rusage[mem=1000]\'';
    
    my $e = $self->{calmde} ? 'e => 1' : 'e => 0';
    
    foreach my $in_bam (@{$self->{in_bams}}) {
        my $base = basename($in_bam);
        my (undef, undef, $recal_bam, $final_bam) = $self->_bam_name_conversion($in_bam);
        
        my $bam_index = "$final_bam.bai";
        next if -s $bam_index;
        
        # run calmd in an LSF call to a temp script
        my $script_name = $self->{fsu}->catfile($lane_path, $self->{prefix}."calmd_$base.pl");
        
        open(my $scriptfh, '>', $script_name) or $self->throw("Couldn't write to temp script $script_name: $!");
        print $scriptfh qq{
use strict;
use VertRes::Wrapper::samtools;

my \$in_bam = '$recal_bam';
my \$final_bam = '$final_bam';

# run calmd
unless (-s \$final_bam) {
    my \$samtools = VertRes::Wrapper::samtools->new(verbose => 1);
    \$samtools->calmd_and_check(\$in_bam, '$self->{reference}', \$final_bam, r => 1, b => 1, $e);
    \$samtools->run_status() >= 1 || die "calmd failed\n";
    \$samtools->index(qq{$final_bam}, qq{$bam_index});
    \$samtools->run_status >= 1 || die "Failed to create $bam_index";
}

exit;
        };
        close $scriptfh;
        
        my $job_name = $self->{prefix}.'calmd_'.$base;
        $self->archive_bsub_files($lane_path, $job_name);
        
        LSF::run($action_lock, $lane_path, $job_name, $self, qq{perl -w $script_name});
    }
    
    $self->{bsub_opts} = $orig_bsub_opts;
    return $self->{No};
}

1;
