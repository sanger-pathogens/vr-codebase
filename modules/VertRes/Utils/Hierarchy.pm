=head1 NAME

VertRes::Utils::Hierarchy - hierarchy utility functions

=head1 SYNOPSIS

use VertRes::Utils::Hierarchy;

my $hierarchy_util = VertRes::Utils::Hierarchy->new();

$hierarchy_util->;

=head1 DESCRIPTION

General utility functions for working on or with team145's data/mapping/release
hierarchy directory structure.

=head1 AUTHOR

Sendu Bala: bix@sendu.me.uk

=cut

package VertRes::Utils::Hierarchy;

use strict;
use warnings;
use VertRes::IO;
use File::Basename;
use File::Spec;
use File::Path;
use File::Copy;
use Cwd 'abs_path';
use VertRes::Parser::sequence_index;
use VertRes::Wrapper::samtools;
use VertRes::Parser::sam;
use VRTrack::VRTrack;
use VRTrack::Lane;
use VRTrack::Library;
use VRTrack::Sample;
use VRTrack::Project;

use base qw(VertRes::Base);

our %study_to_srp = ('Exon-CEU' => 'SRP000033',
                     'Exon-CHB' => 'SRP000033',
                     'Exon-DEN' => 'SRP000033',
                     'Exon-JPT' => 'SRP000033',
                     'Exon-LWK' => 'SRP000033',
                     'Exon-TSI' => 'SRP000033',
                     'Exon-YRI' => 'SRP000033',
                     'LowCov-CEU' => 'SRP000031',
                     'LowCov-CHB' => 'SRP000031',
                     'LowCov-JPT' => 'SRP000031',
                     'LowCov-YRI' => 'SRP000031',
                     'Trio-CEU' => 'SRP000032',
                     'Trio-YRI' => 'SRP000032');

our %platform_aliases = (ILLUMINA => 'SLX',
                         Illumina => 'SLX',
                         LS454 => '454');

our $DEFAULT_DB_SETTINGS = {host => 'mcs4a',
                            port => 3306,
                            user => 'vreseq_ro',
                            database => 'g1k_meta'};

=head2 new

 Title   : new
 Usage   : my $obj = VertRes::Utils::Hierarchy->new();
 Function: Create a new VertRes::Utils::Hierarchy object.
 Returns : VertRes::Utils::Hierarchy object
 Args    : n/a

=cut

sub new {
    my ($class, @args) = @_;
    
    my $self = $class->SUPER::new(@args);
    
    return $self;
}

=head2 parse_lane

 Title   : parse_lane
 Usage   : my %info = $obj->parse_lane('/path/to/lane');
 Function: Extract information about a lane based on its location in the
           hierarchy directory structure.
 Returns : hash with keys study, sample, platform, library and lane.
 Args    : a directory path

=cut

sub parse_lane {
    my ($self, $lane_path) = @_;
    
    my @dirs = File::Spec->splitdir($lane_path);
    @dirs >= 5 || $self->throw("lane path '$lane_path' wasn't valid");
    
    my ($study, $sample, $platform, $library, $lane) = @dirs[-5..-1];
    
    return (study => $study, sample => $sample, platform => $platform,
            library => $library, lane => $lane);
}

=head2 lane_info

 Title   : lane_info
 Usage   : my $path = $obj->lane_info('lane_name');
 Function: Get information about a lane from the VRTrack meta database.
 Returns : hash of information, with keys:
           hierarchy_path => string,
           project        => string,
           sample         => string,
           individual     => string,
           individual_acc => string,
           individual_coverage => float, (the coverage of this lane's individual)
           population     => string,
           technology     => string, (aka platform)
           library        => string,
           lane           => string, (aka read group)
           centre         => string, (the sequencing centre name)
           insert_size    => int, (can be undef if this lane is single-ended)
           withdrawn      => boolean,
           imported       => boolean,
           mapped         => boolean,
           vrlane         => VRTrack::Lane object
           (returns undef if lane name isn't in the database)
 Args    : lane name (read group) OR a VRTrack::Lane object.
           Optionally, a hash with key db OR vrtrack to provide the database
           connection info (shown with defaults):
           db => {
            host => 'mcs4a',
            port => 3306,
            user => 'vreseq_ro',
            password => undef,
            database => 'g1k_meta'
           }
           -or-
           vrtrack => VRTrack::VRTrack object

           optionally, the optional args understood by individual_coverage() to
           configure how individual_coverage will be calculated

=cut

sub lane_info {
    my ($self, $lane, %args) = @_;
    
    my ($rg, $vrlane, $vrtrack);
    if (ref($lane) && $lane->isa('VRTrack::Lane')) {
        $vrlane = $lane;
        $vrtrack = $vrlane->vrtrack;
        $rg = $vrlane->hierarchy_name;
    }
    else {
        if ($args{vrtrack}) {
            $vrtrack = $args{vrtrack};
        }
        else {
            my $db = $args{db} || $DEFAULT_DB_SETTINGS;
            $vrtrack = VRTrack::VRTrack->new($db);
        }
        
        $vrlane = VRTrack::Lane->new_by_hierarchy_name($vrtrack, $lane);
        $rg = $lane;
    }
    
    return unless ($rg && $vrlane && $vrtrack);
    
    my %info = (lane => $rg, vrlane => $vrlane);
    
    $info{hierarchy_path} = $vrtrack->hierarchy_path_of_lane_hname($rg);
    $info{withdrawn} = $vrlane->is_withdrawn;
    $info{imported} = $vrlane->is_processed('import');
    $info{mapped} = $vrlane->is_processed('mapped');
    
    my %objs = $self->lane_hierarchy_objects($vrlane);
    
    $info{insert_size} = $objs{library}->insert_size;
    $info{library} = $objs{library}->hierarchy_name || $self->throw("library name wasn't known for $rg");
    $info{centre} = $objs{centre}->name || $self->throw("sequencing centre wasn't known for $rg");
    $info{technology} = $objs{platform}->name || $self->throw("sequencing platform wasn't known for $rg");
    $info{sample} = $objs{sample}->name || $self->throw("sample name wasn't known for $rg");
    $info{individual} = $objs{individual}->name || $self->throw("individual name wasn't known for $rg");
    $info{individual_acc} = $objs{individual}->acc || $self->throw("sample accession wasn't known for $rg");
    $info{individual_coverage} = $self->hierarchy_coverage(individual => [$info{individual}],
                                                           vrtrack => $vrtrack,
                                                           $args{genome_size} ? (genome_size => $args{genome_size}) : (),
                                                           $args{gt_confirmed} ? (gt_confirmed => $args{gt_confirmed}) : (),
                                                           $args{qc_passed} ? (qc_passed => $args{qc_passed}) : (),
                                                           $args{mapped} ? (mapped => $args{mapped}) : ());
    $info{population} = $objs{population}->name;
    $info{project} = $objs{project}->hierarchy_name;
    
    return %info;
}

=head2 lane_hierarchy_objects

 Title   : lane_hierarchy_objects
 Usage   : my $objects = $obj->lane_hierarchy_objects($lane);
 Function: Get all the parent objects of a lane, from the library up to the
           project.
 Returns : hash with these key and value pairs:
           project => VRTrack::Project object
           sample => VRTrack::Sample object
           individual => VRTrack::Individual object
           population => VRTrack::Population object
           platform => VRTrack::Seq_tech object
           centre => VRTrack::Seq_centre object
           library => VRTrack::Library object
 Args    : VRTrack::Lane object

=cut

sub lane_hierarchy_objects {
    my ($self, $vrlane) = @_;
    
    my $vrtrack = $vrlane->vrtrack;
    my $lib = VRTrack::Library->new($vrtrack, $vrlane->library_id);
    my $sc = $lib->seq_centre;
    my $st = $lib->seq_tech;
    my $sample = VRTrack::Sample->new($vrtrack, $lib->sample_id);
    my $individual = $sample->individual;
    my $pop = $individual->population;
    my $project_obj = VRTrack::Project->new($vrtrack, $sample->project_id);
    
    return (project => $project_obj,
            sample => $sample,
            individual => $individual,
            population => $pop,
            platform => $st,
            centre => $sc,
            library => $lib);
}

=head2 hierarchy_coverage

 Title   : hierarchy_coverage
 Usage   : my $coverage = $obj->hierarchy_coverage(sample => ['NA19239'],
                                                   genome_size => 3e9);
 Function: Discover the sequencing coverage calculated over certain lanes.
 Returns : float
 Args    : At least one hierarchy level as a key, and an array ref of names
           as values, eg. sample => ['NA19239'], platform => ['SLX', '454'].
           Valid key levels are project, sample, individual, population,
           platform, centre and library. (With no options at all, coverage will
           be calculated over all lanes in the database)
           -OR-
           A special mode can be activated by supplying a single lane name with
           the key lane, and a desired level with the level key, eg.:
           lane => 'lane_name', level => 'individual'. This would calculate the
           coverage of all the lanes that belong to the individual that the
           supplied lane belonged to. Caching allows 

           plus optional hash:
           genome_size => int (total genome size in bp; default 3e9)
           gt_confirmed => boolean (only consider genotype confirmed lanes;
                                    default false)
           qc_passed => boolean (only consider qc passed lanes; default false)
           mapped => boolean (coverage of mapped bases; default false: coverage
                              of total bases)

           Optionally, a hash with key db OR vrtrack to provide the database
           connection info (shown with defaults):
           db => {
            host => 'mcs4a',
            port => 3306,
            user => 'vreseq_ro',
            password => undef,
            database => 'g1k_meta'
           }
           -or-
           vrtrack => VRTrack::VRTrack object

=cut

sub hierarchy_coverage {
    my ($self, %args) = @_;
    my $genome_size = delete $args{genome_size} || 3e9;
    my $gt = delete $args{gt_confirmed} ? 1 : 0;
    my $qc = delete $args{qc_passed} ? 1 : 0;
    my $mapped = delete $args{mapped} ? 1 : 0;
    
    if (exists $args{lane} || exists $args{level}) {
        my $lane = delete $args{lane};
        my $level = delete $args{level};
        $self->throw("Both lane and level options must be supplied if either of them are") unless $lane && $level;
        
        my @levels = qw(project sample individual population platform centre library);
        foreach my $valid_level (@levels) {
            $self->throw("'$valid_level' option is mutually exclusive of lane&level") if exists $args{$valid_level};
        }
        my %levels = map { $_ => 1 } @levels;
        $self->throw("Supplied level '$level' wasn't valid") unless exists $levels{$level};
        
        my $db = $args{db} || $DEFAULT_DB_SETTINGS;
        my $vrtrack = VRTrack::VRTrack->new($db);
        my $vrlane = VRTrack::Lane->new_by_name($vrtrack, $lane) || $self->throw("Could not get a lane from the db with name '$lane'");
        
        my %objs = $self->lane_hierarchy_objects($vrlane);
        $self->throw("Could not get the $level of lane $lane") unless defined $objs{$level};
        
        $args{$level} = [$objs{$level}->name];
    }
    
    my @store = ($gt, $qc, $mapped);
    while (my ($key, $val) = each %args) {
        unless (ref($val)) {
            push(@store, $key, $val);
        }
        else {
            if (ref($val) eq 'ARRAY') {
                push(@store, $key, @{$val});
            }
            else {
                push(@store, $key);
                while (my ($sub_key, $sub_val) = each %{$val}) {
                    push(@store, $sub_key, $sub_val);
                }
            }
        }
    }
    my $store = join(",", sort @store);
    
    unless (defined $self->{_cover_bases}->{$store}) {
        my @lanes = $self->get_lanes(%args);
        @lanes || return 0;
        my $bps = 0;
        
        # sum raw bases for all the qc passed, gt confirmed and not withdrawn
        # lanes
        foreach my $lane (@lanes) {
            next if $lane->is_withdrawn;
            if ($gt) {
                next unless ($lane->genotype_status && $lane->genotype_status eq 'confirmed');
            }
            if ($qc) {
                next unless ($lane->qc_status && $lane->qc_status eq 'passed');
            }
            
            my $bp = $lane->raw_bases;
            
            if ($mapped) {
                my $mapstats = $lane->latest_mapping;
                
                if ($mapstats && $mapstats->raw_bases){
                    if ($mapstats->genotype_ratio) {
                        # this is a QC mapped lane, so we make a projection
                        $bps += $bp * ($mapstats->rmdup_bases_mapped / $mapstats->raw_bases);
                    }
                    else {
                        # this is a fully mapped lane, so we know the real answer
                        $bps += $mapstats->bases_mapped;
                    }
                }
                else {
                    $bps += $bp * 0.9; # not sure what else to do here?
                }
            }
            else {
                $bps += $bp;
            }
        }
        
        $self->{_cover_bases}->{$store} = $bps;
    }
    
    return sprintf('%.2f', $self->{_cover_bases}->{$store} / $genome_size);
}

=head2 get_lanes

 Title   : get_lanes
 Usage   : my @lanes = $obj->get_lanes(sample => ['NA19239']);
 Function: Get all the lanes under certain parts of the hierarchy, excluding
           withdrawn lanes.
 Returns : list of VRTrack::Lane objects
 Args    : At least one hierarchy level as a key, and an array ref of names
           as values, eg. sample => ['NA19239'], platform => ['SLX', '454'].
           Valid key levels are project, sample, individual, population,
           platform, centre and library. (With no options at all, all active
           lanes in the database will be returned)

           Optionally, a hash with key db OR vrtrack to provide the database
           connection info (shown with defaults):
           db => {
            host => 'mcs4a',
            port => 3306,
            user => 'vreseq_ro',
            password => undef,
            database => 'g1k_meta'
           }
           -or-
           vrtrack => VRTrack::VRTrack object

=cut

sub get_lanes {
    my ($self, %args) = @_;
    
    my $vrtrack;
    if ($args{vrtrack}) {
        $vrtrack = $args{vrtrack};
    }
    else {
        my $db = $args{db} || $DEFAULT_DB_SETTINGS;
        $vrtrack = VRTrack::VRTrack->new($db);
    }
    
    my @good_lanes;
    foreach my $project (@{$vrtrack->projects}) {
        my $ok = 1;
        if (defined ($args{project})) {
            $ok = 0;
            foreach my $name (@{$args{project}}) {
                if ($name eq $project->name || $name eq $project->hierarchy_name || $name eq $project->study->acc) {
                    $ok = 1;
                    last;
                }
            }
        }
        $ok || next;
        
        foreach my $sample (@{$project->samples}) {
            my $ok = 1;
            if (defined ($args{sample})) {
                $ok = 0;
                foreach my $name (@{$args{sample}}) {
                    if ($name eq $sample->name) {
                        $ok = 1;
                        last;
                    }
                }
            }
            $ok || next;
            
            my %objs;
            $objs{individual} = $sample->individual;
            $objs{population} = $objs{individual}->population;
            
            my ($oks, $limits) = (0, 0);
            foreach my $limit (qw(individual population)) {
                if (defined $args{$limit}) {
                    $limits++;
                    my $ok = 0;
                    foreach my $name (@{$args{$limit}}) {
                        if ($name eq $objs{$limit}->name || ($objs{$limit}->can('hierarchy_name') && $name eq $objs{$limit}->hierarchy_name)) {
                            $ok = 1;
                            last;
                        }
                    }
                    $oks += $ok;
                }
            }
            next unless $oks == $limits;
            
            foreach my $library (@{$sample->libraries}) {
                my $ok = 1;
                if (defined ($args{library})) {
                    $ok = 0;
                    foreach my $name (@{$args{library}}) {
                        if ($name eq $library->name || $name eq $library->hierarchy_name) {
                            $ok = 1;
                            last;
                        }
                    }
                }
                $ok || next;
                
                my %objs;
                $objs{centre} = $library->seq_centre;
                $objs{platform} = $library->seq_tech;
                
                my ($oks, $limits) = (0, 0);
                foreach my $limit (qw(centre platform)) {
                    if (defined $args{$limit}) {
                        $limits++;
                        my $ok = 0;
                        foreach my $name (@{$args{$limit}}) {
                            if ($name eq $objs{$limit}->name) {
                                $ok = 1;
                                last;
                            }
                        }
                        $oks += $ok;
                    }
                }
                next unless $oks == $limits;
                
                push(@good_lanes, @{$library->lanes});
            }
        }
    }
    
    return @good_lanes;
}

=head2 check_lanes_vs_sequence_index

 Title   : check_lanes_vs_sequence_index
 Usage   : my $ok = $obj->check_lanes_vs_sequence_index(['/lane/paths', ...],
                                                        'sequence.index');
 Function: Check that the given lanes reside in the correct part of the
           hierarchy by checking the information in the sequence.index file.
 Returns : boolean (true if all lanes agree with the sequence index)
 Args    : reference to a list of lane paths to check, sequence.index filename,
           boolean to turn on optional checking to see if you're missing any
           lanes that you should have according to the sequence.index

=cut

sub check_lanes_vs_sequence_index {
    my ($self, $lanes, $sequence_index, $check_for_missing) = @_;
    
    my $sip = VertRes::Parser::sequence_index->new(file => $sequence_index,
                                                   verbose => $self->verbose);
    
    my $all_ok = 1;
    my @lane_ids;
    foreach my $lane_path (@{$lanes}) {
        my %lane_info = $self->parse_lane($lane_path);
        my $lane_id = $lane_info{lane};
        push(@lane_ids, $lane_id);
        
        my $sample_name = $sip->lane_info($lane_id, 'sample_name');
        my $platform = $sip->lane_info($lane_id, 'INSTRUMENT_PLATFORM');
        if (exists $platform_aliases{$platform}) {
            $platform = $platform_aliases{$platform};
        }
        my $library = $sip->lane_info($lane_id, 'LIBRARY_NAME');
        $library =~ s/\s/_/;
        my $expected_path = join('/', $sample_name, $platform, $library);
        
        # check this lane is even in the sequence.index; $sip will warn if not
        unless ($sample_name) {
            $all_ok = 0;
            next;
        }
        
        # check this lane hasn't been withdrawn
        my $withdrawn = $sip->lane_info($lane_id, 'WITHDRAWN');
        if ($withdrawn) {
            $self->warn("withdrawn: $lane_id ($lane_path)");
            $all_ok = 0;
            next;
        }
        
        # study swaps
        my $given_study = $study_to_srp{$lane_info{study}} || $lane_info{study};
        my $study = $sip->lane_info($lane_id, 'study_id');
        unless ($study eq $given_study) {
            $self->warn("study swap: $study vs $given_study for $lane_id ($lane_path -> $expected_path)");
            $all_ok = 0;
        }
        
        # sample swaps
        unless ($sample_name eq $lane_info{sample}) {
            $self->warn("sample swap: $sample_name vs $lane_info{sample} for $lane_id ($lane_path -> $expected_path)");
            $all_ok = 0;
        }
        
        # platform swaps
        unless ($platform eq $lane_info{platform}) {
            $self->warn("platform swap: $platform vs $lane_info{platform} for $lane_id ($lane_path -> $expected_path)");
            $all_ok = 0;
        }
        
        # library swaps
        unless ($library eq $lane_info{library}) {
            $self->warn("library swap: $library vs $lane_info{library} for $lane_id ($lane_path -> $expected_path)");
            $all_ok = 0;
        }
    }
    
    if ($check_for_missing) {
        my @lanes = $sip->get_lanes(ignore_withdrawn => 1);
        
        # if we only ignore lines with withdrawn, it doesn't stop us picking up
        # lanes that were both withdrawn and not withdrawn. Filter those out as
        # well:
        my @expected_lanes;
        foreach my $lane (@lanes) {
            my $withdrawn = $sip->lane_info($lane, 'withdrawn');
            if ($withdrawn) {
                $self->warn("lane $lane was both withdrawn and not withdrawn - treating it as withdrawn");
            }
            else {
                push(@expected_lanes, $lane);
            }
        }
        
        # uniquify
        my %expected_lanes = map { $_ => 1 } @expected_lanes;
        @expected_lanes = sort keys %expected_lanes;
        
        my %actual_lanes = map { $_ => 1 } @lane_ids;
        
        foreach my $lane (@expected_lanes) {
            unless (exists $actual_lanes{$lane}) {
                $self->warn("missing: $lane was in the sequence.index but not in the supplied list of lanes");
                $all_ok = 0;
            }
        }
    }
    
    return $all_ok;
}

=head2 fix_simple_swaps

 Title   : fix_simple_swaps
 Usage   : my $swapped = $obj->fix_simple_swaps(['/lane/paths', ...],
                                                'sequence.index');
 Function: For lanes that check_lanes_vs_sequence_index() would complain
           suffered from a swap, moves the lane to the correct part of the
           hierarchy.
 Returns : int (the number of swaps fixed)
 Args    : reference to a list of lane paths to check, sequence.index filename

=cut

sub fix_simple_swaps {
    my ($self, $lanes, $sequence_index) = @_;
    
    my $sip = VertRes::Parser::sequence_index->new(file => $sequence_index,
                                                   verbose => 0);
    
    my $fixed = 0;
    my @lane_ids;
    foreach my $lane_path (@{$lanes}) {
        my %lane_info = $self->parse_lane($lane_path);
        my $lane_id = $lane_info{lane};
        push(@lane_ids, $lane_id);
        
        my $sample_name = $sip->lane_info($lane_id, 'sample_name');
        next unless $sample_name;
        next if $sip->lane_info($lane_id, 'WITHDRAWN');
        
        my $platform = $sip->lane_info($lane_id, 'INSTRUMENT_PLATFORM');
        if (exists $platform_aliases{$platform}) {
            $platform = $platform_aliases{$platform};
        }
        my $library = $sip->lane_info($lane_id, 'LIBRARY_NAME');
        $library =~ s/\s/_/;
        
        # can't handle study swaps, because we don't know which study directory
        # we'd need to move to
        
        # sample swaps
        unless ($sample_name eq $lane_info{sample}) {
            $fixed += $self->_fix_simple_swap($lane_path, $sample_name, $platform, $library);
            next;
        }
        
        # platform swaps
        unless ($platform eq $lane_info{platform}) {
            $fixed += $self->_fix_simple_swap($lane_path, $sample_name, $platform, $library);
            next;
        }
        
        # library swaps
        unless ($library eq $lane_info{library}) {
            $fixed += $self->_fix_simple_swap($lane_path, $sample_name, $platform, $library);
            next;
        }
    }
    
    return $fixed;
}

sub _fix_simple_swap {
    my ($self, $old, $new_sample, $new_platform, $new_library) = @_;
    
    $old = abs_path($old);
    my @dirs = File::Spec->splitdir($old);
    @dirs >= 5 || $self->throw("lane path '$old' wasn't valid");
    my ($sample, $platform, $library, $lane) = splice(@dirs, -4);
    
    # do the swap by moving the lane dir
    my $new = File::Spec->catdir(@dirs, $new_sample, $new_platform, $new_library, $lane);
    if (-d $new) {
        $self->warn("Wanted to swap $old -> $new, but the destination already exists");
        return 0;
    }
    else {
        my $parent = File::Spec->catdir(@dirs, $new_sample, $new_platform, $new_library);
        mkpath($parent);
        $self->throw("Could not create path $parent") unless -d $parent;
    }
    move($old, $new);
    
    # did we just empty any of the parent directories? if so, remove them
    my $parent = File::Spec->catdir(@dirs, $sample, $platform, $library);
    $self->_remove_empty_parent($parent);
    $parent = File::Spec->catdir(@dirs, $sample, $platform);
    $self->_remove_empty_parent($parent);
    $parent = File::Spec->catdir(@dirs, $sample);
    $self->_remove_empty_parent($parent);
    
    return 1;
}

sub _remove_empty_parent {
    my ($self, $parent) = @_;
    
    opendir(my $pfh, $parent) || $self->throw("Could not open dir $parent");
    my $things = 0;
    foreach (readdir($pfh)) {
        next if /^\.+$/;
        $things++;
    }
    
    if ($things == 0) {
        system("rm -fr $parent");
    }
}

=head2 create_release_hierarchy

 Title   : create_release_hierarchy
 Usage   : my $ok = $obj->create_release_hierarchy(\@abs_lane_paths,
                                                   '/path/to/release');
 Function: Given a list of absolute paths to mapped lanes in a mapping
           hierarchy, creates a release hierarchy with mapped bams symlinked
           across.
           NB: You should probably call check_lanes_vs_sequence_index() on the
           the input lanes beforehand.
 Returns : list of absolute paths to the bam symlinks in the created release
           hierarchy
 Args    : reference to a list of mapped lane paths, base path you want to
           build the release hierarchy in

=cut

sub create_release_hierarchy {
    my ($self, $lane_paths, $release_dir) = @_;
    
    unless (-d $release_dir) {
        mkdir($release_dir) || $self->throw("Unable to create base release directory: $!");
    }
    my $io = VertRes::IO->new();
    
    my @all_linked_bams;
    
    foreach my $lane_path (@{$lane_paths}) {
        # setup release lane
        my @dirs = File::Spec->splitdir($lane_path);
        @dirs >= 5 || $self->throw("lane path '$lane_path' wasn't valid");
        my $release_lane_path = $io->catfile($release_dir, @dirs[-5..-1]);
        mkpath($release_lane_path);
        -d $release_lane_path || $self->throw("Unable to make release lane '$release_lane_path'");
        
        # symlink bams
        my @linked_bams;
        foreach my $ended ('pe', 'se') {
            my $bam_file = "${ended}_raw.sorted.bam";
            my $source = $io->catfile($lane_path, $bam_file);
            
            if (-s $source) {
                my $destination = $io->catfile($release_lane_path, $bam_file);
                symlink($source, $destination) || $self->throw("Couldn't symlink $source -> $destination");
                push(@linked_bams, $destination);
            }
        }
        
        # old mapping pipeline didn't create pe/se bams
        unless (@linked_bams) {
            my $legacy_bam_file = 'raw.sorted.bam';
            my $source = $io->catfile($lane_path, $legacy_bam_file);
            if (-s $source) {
                # figure out if it was generated from paired or single ended reads
                my $meta_file = $io->catfile($lane_path, 'meta.info');
                if (-s $meta_file) {
                    $io->file($meta_file);
                    my $fh = $io->fh;
                    my %reads;
                    while (<$fh>) {
                        if (/^read(\d)/) {
                            $reads{$1} = 1;
                        }
                    }
                    $io->close;
                    
                    my $ended;
                    if ($reads{1} && $reads{2}) {
                        $ended = 'pe';
                    }
                    elsif ($reads{0}) {
                        $ended = 'se';
                    }
                    else {
                        $self->throw("Couldn't determine if reads were single or paired end in lane '$lane_path'");
                    }
                    
                    my $bam_file = "${ended}_raw.sorted.bam";
                    my $destination = $io->catfile($release_lane_path, $bam_file);
                    symlink($source, $destination) || $self->throw("Couldn't symlink $source -> $destination");
                    push(@linked_bams, $destination);
                }
            }
        }
        
        unless (@linked_bams) {
            $self->throw("Mapping lane '$lane_path' contained no linkable bam files!");
        }
        
        push(@all_linked_bams, @linked_bams);
    }
    
    return @all_linked_bams;
}

=head2 dcc_filename

 Title   : dcc_filename
 Usage   : my $filename = $obj->dcc_filename('/abs/path/to/platform/release.bam');
 Function: Get the DCC filename of a bam file.
 Returns : string (filename without .bam suffix)
 Args    : absolute path to a platform-level release bam file

=cut

sub dcc_filename {
    my ($self, $file) = @_;
    
    # NAXXXXX.[chromN].technology.[center].algorithm.study_id.YYYY_MM.bam
    # the date "should represent when the alignment was carried out"
    # http://1000genomes.org/wiki/doku.php?id=1000_genomes:dcc:filenames
    
    my ($dcc_filename, $study, $sample, $platform);
    
    # view the bam header
    my $stw = VertRes::Wrapper::samtools->new(quiet => 1);
    $stw->run_method('open');
    my $view_fh = $stw->view($file, undef, H => 1);
    $view_fh || $self->throw("Failed to samtools view '$file'");
    
    my $ps = VertRes::Parser::sam->new(fh => $view_fh);
    
    ($study, $sample, $platform) = ('unknown_study', 'unknown_sample', 'unknown_platform');
    my %techs;
    my %readgroup_info = $ps->readgroup_info();
    while (my ($rg, $info) = each %readgroup_info) {
        # there should only be one of these, so we just keep resetting it
        # (there's no proper tag for holding study, so the mapping pipeline
        # sticks the study into the description tag 'DS')
        $study = $info->{DS} || 'unknown_study';
        $sample = $info->{SM} || 'unknown_sample';
        
        # might be more than one of these if we're a sample-level bam.
        # DCC puts eg. 'ILLUMINA' in the sequence.index files but the
        # filename format expects 'SLX' etc.
        $platform = $info->{PL};
        if ($platform =~ /illumina/i) {
            $platform = 'SLX';
        }
        elsif ($platform =~ /solid/i) {
            $platform = 'SOLID';
        }
        elsif ($platform =~ /454/) {
            $platform = '454';
        }
        $techs{$platform}++;
    }
    
    # old bams don't have study in DS tag
    if ($study eq 'unknown_study') {
        my (undef, $bam_dir) = fileparse($file);
        my @dirs = File::Spec->splitdir($bam_dir);
        if ($dirs[-1] eq '') {
            pop @dirs;
        }
        my $study_dir = $dirs[-3] || '';
        if ($study_dir =~ /LowCov/) {
            $study = 'SRP000031';
        }
        elsif ($study_dir =~ /Trio/) {
            $study = 'SRP000032';
        }
        elsif ($study_dir =~ /Exon/) {
            $study = 'SRP000033';
        }
    }
    
    # SOLID bams are not made by us and have unreliable headers; we'll need to
    # cheat and get the info from the filesystem
    if ($sample eq 'unknown_sample') {
        my (undef, $bam_dir) = fileparse($file);
        my @dirs = File::Spec->splitdir($bam_dir);
        if ($dirs[-1] eq '') {
            pop @dirs;
        }
        if ($dirs[-1] eq 'SOLID') {
            $platform = 'SOLID';
            $sample = $dirs[-2];
            $study = $dirs[-3];
            if (exists $study_to_srp{$study}) {
                $study = $study_to_srp{$study};
            }
            else {
                $self->warn("bam $file detected as being in unknown study '$study'");
            }
        }
    }
    
    if (keys %techs > 1) {
        $platform = '';
    }
    else {
        $platform .= '.';
    }
    
    my $bamname = basename($file);
    my $chrom = '';
    if ($bamname =~ /^(\d+|[XY]|MT)/) {
        $chrom = "chrom$1.";
    }
    
    my $mtime = (stat($file))[9];
    my ($month, $year) = (localtime($mtime))[4..5];
    $year += 1900;
    $month = sprintf("%02d", $month + 1);
    
    my $algorithm = $ps->program || 'unknown_algorithm';
    if ($algorithm eq 'unknown_algorithm') {
        if ($platform =~ /SLX/) {
            $algorithm = 'maq';
        }
        elsif ($platform =~ /454/) {
            $algorithm = 'ssaha2';
        }
        elsif ($platform =~ /SOLID/) {
            $algorithm = 'corona';
        }
    }
    
    $dcc_filename = "$sample.$chrom$platform$algorithm.$study.${year}_$month";
    
    return $dcc_filename;
}

=head2 netapp_lane_path

 Title   : netapp_lane_path
 Usage   : my $path = $obj->netapp_lane_path('lane_name');
 Function: 
 Returns : path string
 Args    : 

=cut

1;
