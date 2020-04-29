#!/usr/bin/env perl
use strict;
use warnings;
use Data::Dumper;
use Test::Files;

BEGIN { unshift( @INC, './lib' ) }

BEGIN {
    use Test::Most;
    use_ok('Bio::VertRes::Permissions::Groups');
}

ok(my $obj = Bio::VertRes::Permissions::Groups->new(), 'initialise obj');
ok(my $groups = $obj->groups(),'get groups');
is($groups->[0], $obj->is_member_of_group($groups->[0]),'Found group');

my $groups_with_spaces = "some_group_with_doesnt_exist ".$groups->[0];
is($groups->[0], $obj->is_member_of_group($groups_with_spaces),'Found group where its space separated');

is(undef, $obj->is_member_of_group("some_group_with_doesnt_exist"),'group which doesnt exist should return undef');

is(undef, $obj->is_member_of_group(""),'group which is empty should return undef');
is(undef, $obj->is_member_of_group(undef),'group which is undef should return undef');


done_testing();
