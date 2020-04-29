#!/usr/bin/env perl
use strict;
use warnings;
use Data::Dumper;
use Test::Files;
use File::Temp qw/ tempfile tempdir /;

BEGIN { unshift( @INC, './lib' ) }

BEGIN {
    use Test::Most;
    use_ok('Bio::VertRes::Permissions');
    use Bio::VertRes::Permissions::Groups;
}
my $obj;

my $groups_obj = Bio::VertRes::Permissions::Groups->new();
my $groups     = $groups_obj->groups();

my $username = $ENV{LOGNAME} || $ENV{USER} || getpwuid($<);

# create some temp directories and files
my $num_temp_directories = 3;
my $num_temp_files       = 3;
my @temp_dir_objs;
my @temp_dirs;
my @temp_files;
for ( my $i = 0 ; $i < $num_temp_directories ; $i++ ) {
    my $temp_dir_obj = File::Temp->newdir();
    push( @temp_dir_objs, $temp_dir_obj );
    push( @temp_dirs,     $temp_dir_obj->dirname );

    system("touch ".$temp_dir_obj->dirname. "/file_1");
	push(@temp_files, $temp_dir_obj->dirname. "/file_1");
}

my $octal_permissions = 0700;
for my $group ( @{$groups} ) {
	next if $group =~ /^[\d]+$/;

    ok($obj = Bio::VertRes::Permissions->new(input_directories => \@temp_dirs, partition_level => 2, threads => 1, group => $group, user => $username, octal_permissions => $octal_permissions), "initialise permissions obj for group $group");
    ok($obj->update_permissions(), "update permissions for group $group");

	for my $temp_file (@temp_files)
	{
		my ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size, $atime,$mtime,$ctime,$blksize,$blocks) = stat($temp_file);
		is(getgrgid($gid),$group, "group has been correctly set for $temp_file to $group" );
		is($mode & 07777, 0700, "Check file permissions correct for $group");
	}
}

done_testing();
