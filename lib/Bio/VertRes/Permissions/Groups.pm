package Bio::VertRes::Permissions::Groups;

# ABSTRACT:  Groups for the current user

=head1 SYNOPSIS

Groups for the current user
   use Bio::VertRes::Permissions::Groups;
   
   my $obj = Bio::VertRes::Permissions::Groups->new();
   $obj->groups();
   $obj->is_member_of_group();

=cut

use Moose;
with 'Bio::VertRes::Permissions::LoggerRole';

has 'groups' => ( is => 'ro', isa => 'ArrayRef', lazy => 1, builder => '_build_groups' );

sub _build_groups {
    my ($self) = @_;
	# Unix::Groups fails to compile on OSX mountain lion
	my $raw_groups_output = `groups`;
	my @groups = split(/[\s\t]/, $raw_groups_output);
	$self->logger->info( "The current users groups are: " . join( ',', @groups ) );

    return \@groups;
}

sub is_member_of_group {
    my ( $self, $input_group ) = @_;
	return undef if((!defined($input_group)) || $input_group eq "");
	
	# The input group can be a space separated list of group names, so check all and return when 1 is found.
	my @input_groups = split(/[\s\t]+/,$input_group);
	
	for my $current_group (@input_groups)
	{
        if ( grep { $_ eq $current_group } @{ $self->groups } ) {
	    	$self->logger->info("The current users groups include $input_group");
            return $current_group;
        }
    }
	$self->logger->info("The current users groups do not include $input_group");
    return undef;
}

no Moose;
__PACKAGE__->meta->make_immutable;

1;
