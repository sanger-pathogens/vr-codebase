# A dummy pipeline that has no interactions with databases

package VertRes::Pipelines::DummyJob;
use base qw(VertRes::Pipeline);

use strict;
use warnings;

our @actions =
(
    {
        'name'     => 'hello',
        'action'   => \&hello,
        'requires' => \&hello_requires, 
        'provides' => \&hello_provides,
    },
    {
        'name'     => 'world',
        'action'   => \&world,
        'requires' => \&world_requires, 
        'provides' => \&world_provides,
    },
    {
        'name'     => 'extra',
        'action'   => \&extra,
        'requires' => \&extra_requires, 
        'provides' => \&extra_provides,
    },
);

our $options = 
{
    'Hello' => 'Hello',
    'World' => 'World',
    'Extra' => 'Extra'
};


# --------- OO stuff --------------

sub new 
{
    print "In OliHack\n";
    my ($class, @args) = @_;
    my $self = $class->SUPER::new(%$options,'actions'=>\@actions,@args);
    use Data::Dumper;
    print Dumper($self);

    return $self;
}


# --------- hello --------------

sub hello_requires
{
    my ($self,$lane) = @_;
    my @requires = ();
    return \@requires;
}

sub hello_provides
{
    my ($self,$lane) = @_;
    my @provides = ('hello.txt');
    return \@provides;
}

sub hello
{
    my ($self,$lane_path,$action_lock) = @_;
    print("lane path:".$lane_path."\n");
    open(my $fh, '>', "$lane_path/hello.txt") or Utils::error("$lane_path/hello.txt: $!");
    print $fh $$self{'Hello'}, "\n";
    close $fh;

    return $$self{'Yes'};
}


# --------- world --------------

sub world_requires
{
    my ($self,$lane) = @_;
    my @requires = ('hello.txt');
    return \@requires;
}

sub world_provides
{
    my ($self,$lane) = @_;
    my @provides = ('world.txt');
    return \@provides;
}

sub world
{
    my ($self,$lane_path,$action_lock) = @_;
    open(my $fh, '>', "$lane_path/world.txt") or Utils::error("$lane_path/world.txt: $!");
    print $fh $$self{'World'}, "\n";
    close $fh;

    return $$self{'Yes'};
}

# --------- extra --------------

sub extra_requires
{
    my ($self,$lane) = @_;
    my @requires = ('world.txt');
    return \@requires;
}

sub extra_provides
{
    my ($self,$lane) = @_;
    my @provides = ('extra.txt');
    return \@provides;
}

sub extra
{
    my ($self,$lane_path,$action_lock) = @_;
    open(my $fh, '>', "$lane_path/extra.txt") or Utils::error("$lane_path/extra.txt: $!");
    print $fh $$self{'Extra'}, "\n";
    print $fh "other stuff...\n";
    close $fh;

    return $$self{'Yes'};
}

1;

