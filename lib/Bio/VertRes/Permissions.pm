package Bio::VertRes::Permissions;

# ABSTRACT:  Modify the permissions of all files in an input set of directories

=head1 SYNOPSIS

Modify the permissions of all files in an input set of directories
   use Bio::VertRes::Permissions;
   
   my $obj = Bio::VertRes::Permissions->new(input_directories => \@directories, partition_level => 2, threads => 1, group => 'abc', user => 'efg', octal_permissions => 0700);
   $obj->update_permissions();

=cut

use Moose;
use Bio::VertRes::Permissions::PartitionDirectories;
use Bio::VertRes::Permissions::ModifyPermissions;
use Parallel::ForkManager;
with 'Bio::VertRes::Permissions::LoggerRole';

has 'input_directories'                  => ( is => 'ro', isa => 'ArrayRef', required => 1 );
has 'partition_level'                    => ( is => 'ro', isa => 'Int',      default  => 2 );
has 'threads_per_disk_array'             => ( is => 'ro', isa => 'Int',      default  => 1 );
has 'num_disk_arrays_to_process_at_once' => ( is => 'ro', isa => 'Int',      default  => 1 );
has 'group'                              => ( is => 'ro', isa => 'Str',      required => 0 );
has 'user'                               => ( is => 'ro', isa => 'Str',      required => 0 );
has 'octal_permissions'                  => ( is => 'ro', isa => 'Num',      default  => 0750 );
has '_partitioned_directories'           => ( is => 'ro', isa => 'ArrayRef', lazy => 1, builder => '_build__partitioned_directories' );

sub _build__partitioned_directories {
    my ($self) = @_;
    return Bio::VertRes::Permissions::PartitionDirectories->new( 
	    input_directories => $self->input_directories, 
	    logger            => $self->logger )
      ->partitioned_directories;
}

sub update_permissions {
    my ($self) = @_;

    my $pm = new Parallel::ForkManager( $self->num_disk_arrays_to_process_at_once );
    for my $disk_array_directories ( @{ $self->_partitioned_directories } ) {
        $pm->start and next;

        my $modify_permissions = Bio::VertRes::Permissions::ModifyPermissions->new(
            input_directories => $disk_array_directories,
            user              => $self->user,
            group             => $self->group,
            octal_permissions => $self->octal_permissions,
            threads           => $self->threads_per_disk_array,
            logger            => $self->logger
        );
        $modify_permissions->update_permissions;

        $pm->finish;
    }
    $pm->wait_all_children;
    return $self;

}

no Moose;
__PACKAGE__->meta->make_immutable;

1;
