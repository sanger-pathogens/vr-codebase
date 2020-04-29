undef $VERSION;

package Bio::VertRes::Permissions::CommandLine::UpdatePipelineFilePermissions;

# ABSTRACT: Update all the permissions of files in a list of directories

=head1 SYNOPSIS

Update all the permissions of files in a list of directories

=cut

use Moose;
use Getopt::Long qw(GetOptionsFromArray);
use Bio::VertRes::Permissions;
use Bio::VertRes::Permissions::ParseFileOfDirectories;
use Bio::VertRes::Permissions::Groups;
with 'Bio::VertRes::Permissions::LoggerRole';

has 'args'                               => ( is => 'ro', isa => 'ArrayRef', required => 1 );
has 'script_name'                        => ( is => 'ro', isa => 'Str',      required => 1 );
has 'help'                               => ( is => 'rw', isa => 'Bool',     default  => 0 );
has 'type'                               => ( is => 'rw', isa => 'Str',      default  => 'file_of_directories' );
has 'type_id'                            => ( is => 'rw', isa => 'Str',      default  => '' );
has 'user'                               => ( is => 'rw', isa => 'Str',      default  => 'pathpipe' );
has 'group'                              => ( is => 'rw', isa => 'Str',      default  => 'pathogen' );
has 'octal_permissions'                  => ( is => 'rw', isa => 'Num',      default  => 0750 );
has 'partition_level'                    => ( is => 'rw', isa => 'Int',      default  => 2 );
has 'threads_per_disk_array'             => ( is => 'rw', isa => 'Int',      default  => 1 );
has 'num_disk_arrays_to_process_at_once' => ( is => 'rw', isa => 'Int',      default  => 1 );
has 'verbose'                            => ( is => 'rw', isa => 'Bool',     default  => 0 );

sub BUILD {
    my ($self) = @_;

    my ( $type, $type_id, $user, $group, $octal_permissions, $partition_level, $threads_per_disk_array, $num_disk_arrays_to_process_at_once,
        $verbose, $help );

    GetOptionsFromArray(
        $self->args,
        't|type=s'                             => \$type,
        'i|type_id=s'                          => \$type_id,
        'u|user=s'                             => \$user,
        'g|group=s'                            => \$group,
        'o|octal_permissions=s'                => \$octal_permissions,
        'l|partition_level=i'                  => \$partition_level,
        'd|threads_per_disk_array=i'             => \$threads_per_disk_array,
        'p|num_disk_arrays_to_process_at_once=i' => \$num_disk_arrays_to_process_at_once,
        'v|verbose'                            => \$verbose,
        'h|help'                               => \$help,
    );

    if ( defined($verbose) ) {
        $self->verbose($verbose);
        $self->logger->level(10000);
    }

    $self->help($help) if ( defined($help) );
    ( !$self->help ) or die $self->usage_text;
	( defined( $type_id) && -e  $type_id ) or die $self->usage_text;

    $self->type($type)                                                             if ( defined($type) );
    $self->type_id($type_id)                                                       if ( defined($type_id) );
    $self->user($user)                                                             if ( defined($user) );
    $self->group($group)                                                           if ( defined($group) );
    $self->octal_permissions(oct($octal_permissions))                                   if ( defined($octal_permissions) );
    $self->partition_level($partition_level)                                       if ( defined($partition_level) );
    $self->threads_per_disk_array($threads_per_disk_array)                         if ( defined($threads_per_disk_array) );
    $self->num_disk_arrays_to_process_at_once($num_disk_arrays_to_process_at_once) if ( defined($num_disk_arrays_to_process_at_once) );
}

sub run {
    my ($self) = @_;

    my $groups_obj = Bio::VertRes::Permissions::Groups->new(logger => $self->logger);
	if(! $groups_obj->is_member_of_group($self->group))
	{
		$self->logger->error("The current user does not have permission to use the input group: ".$self->group);
		die();
	}
    
    my $input_directories =
      Bio::VertRes::Permissions::ParseFileOfDirectories->new( input_file => $self->type_id, logger => $self->logger )->directories();

    my $update_permissions_obj = Bio::VertRes::Permissions->new(
        input_directories                  => $input_directories,
        partition_level                    => $self->partition_level,
        threads_per_disk_array             => $self->threads_per_disk_array,
        num_disk_arrays_to_process_at_once => $self->num_disk_arrays_to_process_at_once,
        group                              => $self->group,
        user                               => $self->user,
        octal_permissions                  => $self->octal_permissions,
        logger                             => $self->logger,
    );
    $update_permissions_obj->update_permissions();
}

sub usage_text {
    my ($self) = @_;

    return <<USAGE;
Usage: update_pipeline_file_permissions [options]
Changes the permissions of all files in a set of directories
  
Options: -t STR    type [file_of_directories]
         -i STR    input filename of directories []
         -u STR    username [pathpipe]
         -g STR    unix group [pathogen]
         -o STR    file permissions in octal [0750]
         -l INT    directory level to split on [2]
         -d INT    threads per disk array [1]
         -p INT    num disk arrays to process in parallel [1]
         -v        verbose output to STDOUT
         -h        this help message
		 
Example: Run with defaults
         update_pipeline_file_permissions -i file_of_directories.txt 

Example: Update group to team81 in parallel
         bsub.py --threads 16 5 log update_pipeline_file_permissions -i file_of_directories.txt -d 4 -p 4 -g team81

USAGE
}

__PACKAGE__->meta->make_immutable;
no Moose;
1;
