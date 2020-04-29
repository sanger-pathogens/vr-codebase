package Bio::VertRes::Permissions::ModifyPermissions;

# ABSTRACT:  Take in a list of directories and update the permissions and owners of all files

=head1 SYNOPSIS

Take in a list of directories and update the permissions and owners of all files
   use Bio::VertRes::Permissions::ModifyPermissions;
   
   my $obj = Bio::VertRes::Permissions::ModifyPermissions->new(input_directories => \@directories, user => 'xyz', group => 'abc', octal_permissions => 0750);
   $obj->update_permissions;


=cut

use Moose;
use Parallel::ForkManager;
use File::Find;
with 'Bio::VertRes::Permissions::LoggerRole';

has 'input_directories' => (
    is       => 'ro',
    isa      => 'ArrayRef',
    required => 1
);
has 'threads'           => ( is => 'ro', isa => 'Int', default  => 1 );
has 'group'             => ( is => 'ro', isa => 'Str', required => 1 );
has 'user'              => ( is => 'ro', isa => 'Str', lazy => 1, builder => '_build_user' );

has '_uid'              => ( is => 'ro', isa => 'Num', lazy => 1, builder => '_build__uid' );
has '_gid'              => ( is => 'ro', isa => 'Num', lazy => 1, builder => '_build__gid' );

has 'octal_permissions' => ( is => 'ro', isa => 'Num', default  => 0750 );

sub BUILD {
    my ($self) = @_;
    $self->logger->info( "Changing file permissions - threads: " . $self->threads );
    $self->logger->info( "Changing file permissions - permissions: " . $self->octal_permissions );
    $self->logger->info( "Changing file permissions - user: " . $self->user );
    $self->logger->info( "Changing file permissions - group: " . $self->group );
}

sub _build__uid
{
	my ($self) = @_;
	my $uid = getpwnam $self->user;
	return $uid;
}

sub _build__gid
{
	my ($self) = @_;
	my $gid = getgrnam $self->group;
	return $gid;
}

sub _build_user
{
	my ($self) = @_;
	return $ENV{LOGNAME} || $ENV{USER} || getpwuid($<);
}

sub _wanted {
    my $self = ${ $_[0] }{self};
    return unless ( defined $File::Find::name );
    return if($File::Find::name =~ /lock$/ );

    my ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size, $atime,$mtime,$ctime,$blksize,$blocks)= stat($File::Find::name);

    my $shifted_permissions = 1023 & $mode;
    if( $shifted_permissions != $self->octal_permissions)
    {
    	chmod $self->octal_permissions, $File::Find::name;
    }

    if($gid != $self->_gid )
    {
       chown $self->_uid, $self->_gid, $_;
    }
}

sub update_permissions {
    my ($self) = @_;

    my $pm = new Parallel::ForkManager( $self->threads );
    for my $directory ( @{ $self->input_directories } ) {
        $pm->start and next;

        find(
            {
                wanted => sub {
                    _wanted( { self => $self } );
                },
                follow => 1
            },
            $directory
        );
        $pm->finish;
    }
    $pm->wait_all_children;
    return $self;
}

no Moose;
__PACKAGE__->meta->make_immutable;

1;
