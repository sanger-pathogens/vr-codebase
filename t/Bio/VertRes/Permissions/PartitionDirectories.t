#!/usr/bin/env perl
use strict;
use warnings;
use Data::Dumper;
use Test::Files;

BEGIN { unshift( @INC, './lib' ) }

BEGIN {
    use Test::Most;
    use_ok('Bio::VertRes::Permissions::PartitionDirectories');
}
my $obj;
my @input_directories = ('/nfs/aaa/1','/nfs/aaa/2','/nfs/ddd/1','/nfs/aaa/3','/nfs/aaa/4','/nfs/bbb/1','/nfs/ccc/1','/nfs/ccc/2',);

ok($obj = Bio::VertRes::Permissions::PartitionDirectories->new(input_directories => \@input_directories, partition_level => 1), 'initialise obj level 1');
is_deeply($obj->partitioned_directories, [['/nfs/aaa/1','/nfs/aaa/2','/nfs/aaa/3','/nfs/aaa/4','/nfs/bbb/1','/nfs/ccc/1','/nfs/ccc/2','/nfs/ddd/1']], 'partition on level 1');

ok($obj = Bio::VertRes::Permissions::PartitionDirectories->new(input_directories => \@input_directories, partition_level => 2), 'initialise obj level 2');
is_deeply($obj->partitioned_directories, [['/nfs/aaa/1','/nfs/aaa/2','/nfs/aaa/3','/nfs/aaa/4'],['/nfs/bbb/1'],['/nfs/ccc/1','/nfs/ccc/2'],['/nfs/ddd/1']], 'partition on level 2');

ok($obj = Bio::VertRes::Permissions::PartitionDirectories->new(input_directories => \@input_directories, partition_level => 3), 'initialise obj level 3');
is_deeply($obj->partitioned_directories, [['/nfs/aaa/1','/nfs/bbb/1','/nfs/ccc/1','/nfs/ddd/1'],['/nfs/aaa/2','/nfs/ccc/2'],['/nfs/aaa/3'],['/nfs/aaa/4']], 'partition on level 3');

ok($obj = Bio::VertRes::Permissions::PartitionDirectories->new(input_directories => \@input_directories, partition_level => 4), 'initialise obj on ungrouped level');
is_deeply($obj->partitioned_directories, [['/nfs/aaa/1','/nfs/aaa/2','/nfs/aaa/3','/nfs/aaa/4','/nfs/bbb/1','/nfs/ccc/1','/nfs/ccc/2','/nfs/ddd/1']], 'directories dont contain level');

done_testing();
