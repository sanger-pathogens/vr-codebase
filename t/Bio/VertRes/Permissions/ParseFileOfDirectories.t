#!/usr/bin/env perl
use strict;
use warnings;
use Data::Dumper;
use Test::Files;

BEGIN { unshift( @INC, './lib' ) }

BEGIN {
    use Test::Most;
    use_ok('Bio::VertRes::Permissions::ParseFileOfDirectories');
}
my $obj;

ok($obj = Bio::VertRes::Permissions::ParseFileOfDirectories->new(input_file => 't/data/ParseFileOfDirectories/empty_file'), 'initialise obj for empty file');
is_deeply($obj->directories(),[],'empty file should not return any directories');

ok($obj = Bio::VertRes::Permissions::ParseFileOfDirectories->new(input_file => 't/data/ParseFileOfDirectories/one_directory'), 'initialise obj for one directory file');
is_deeply($obj->directories(),['t/data/ParseFileOfDirectories/1'],'one directory returned');

ok($obj = Bio::VertRes::Permissions::ParseFileOfDirectories->new(input_file => 't/data/ParseFileOfDirectories/multiple_directories'), 'initialise obj for multiple directories file');
is_deeply($obj->directories(),['t/data/ParseFileOfDirectories/1','t/data/ParseFileOfDirectories/2','t/data/ParseFileOfDirectories/3'],'multiple directories returned');

ok($obj = Bio::VertRes::Permissions::ParseFileOfDirectories->new(input_file => 't/data/ParseFileOfDirectories/file_with_comments'), 'initialise obj for a file with comments and empty lines');
is_deeply($obj->directories(),['t/data/ParseFileOfDirectories/2','t/data/ParseFileOfDirectories/3'],'in a file with comments, only real directories returned');

ok($obj = Bio::VertRes::Permissions::ParseFileOfDirectories->new(input_file => 't/data/ParseFileOfDirectories/directories_dont_exist'), 'initialise obj for a file with directories which dont exist');
is_deeply($obj->directories(),['t/data/ParseFileOfDirectories/1'],'directories which dont exist are ignored');

done_testing();
