package Bio::VertRes::Permissions::PartitionDirectories;

# ABSTRACT:  Split up directories into different disk arrays

=head1 SYNOPSIS

Split up directories into different disk arrays
   use Bio::VertRes::Permissions::PartitionDirectories;
   
   my $obj = Bio::VertRes::Permissions::PartitionDirectories->new(input_directories => \@directories);
   $obj->partitioned_directories();

=cut

use Moose;
with 'Bio::VertRes::Permissions::LoggerRole';

has 'input_directories'       => ( is => 'ro', isa => 'ArrayRef', required => 1 );
has 'partition_level'         => ( is => 'ro', isa => 'Int',      default  => 2 );
has 'partitioned_directories' => ( is => 'ro', isa => 'ArrayRef', lazy     => 1, builder => '_build_partitioned_directories' );

sub _build_partitioned_directories {
    my ($self) = @_;
    my @partitioned_directories;

    my %levels_to_directories;
    for my $directory ( @{ $self->input_directories } ) {
		$self->logger->info( "Checking directory: " . $directory );
        my @directory_parts = split( '/', $directory );
        if ( @directory_parts > $self->partition_level && $directory_parts[ ( $self->partition_level ) ] ne '' ) {
            push( @{ $levels_to_directories{ $directory_parts[ ( $self->partition_level ) ] } }, $directory );
        }
        else {
            push( @{ $levels_to_directories{ungrouped} }, $directory );
        }
    }

    for my $level_name ( sort keys %levels_to_directories ) {
		$self->logger->info( "Sorting directories in partition level: " . $level_name );
        my @sorted_directories = sort @{ $levels_to_directories{$level_name} };
        push( @partitioned_directories, \@sorted_directories );
    }

    return \@partitioned_directories;
}

no Moose;
__PACKAGE__->meta->make_immutable;

1;
