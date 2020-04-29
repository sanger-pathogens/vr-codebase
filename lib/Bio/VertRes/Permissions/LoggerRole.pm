package Bio::VertRes::Permissions::LoggerRole;

# ABSTRACT: A role for Logging

=head1 SYNOPSIS

A role for Logging
   with 'Bio::VertRes::Permissions::LoggerRole';

=cut

use Moose::Role;
use Log::Log4perl qw(:easy);

has 'logger' => ( is => 'ro', lazy => 1, builder => '_build_logger' );

sub _build_logger {
    my ($self) = @_;
    Log::Log4perl->easy_init( level => $ERROR );
    my $logger = get_logger();
    return $logger;
}

1;
