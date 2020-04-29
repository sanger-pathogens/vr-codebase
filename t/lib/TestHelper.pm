package TestHelper;
use Moose::Role;
use Test::Most;
use Data::Dumper;
use File::Slurper qw(read_lines read_text);
use Test::Files;
use Test::Output;

sub stdout_should_have {
    my ( $script_name, $parameters, $expected ) = @_;
    my @input_args = split( " ", $parameters );
    open OLDERR, '>&STDERR';
    eval("use $script_name ;");
    my $returned_values = 0;
    {
        local *STDERR;
        open STDERR, '>/dev/null' or warn "Can't open /dev/null: $!";
        stdout_like { eval("$script_name->new(args => \\\@input_args, script_name => '$script_name')->run;"); } qr/$expected/,
          "got expected text $expected for $parameters";
        close STDERR;
    }
    open STDERR, '>&OLDERR' or die "Can't restore stderr: $!";
    close OLDERR or die "Can't close OLDERR: $!";
}

sub stderr_should_have {
    my ( $script_name, $parameters, $expected ) = @_;
    my @input_args = split( " ", $parameters );
    open OLDOUT, '>&STDOUT';
    eval("use $script_name ;");
    my $returned_values = 0;
    {
        local *STDOUT;
        open STDOUT, '>/dev/null' or warn "Can't open /dev/null: $!";
        stderr_like { eval("$script_name->new(args => \\\@input_args, script_name => '$script_name')->run;"); } qr/$expected/,
          "got expected text $expected for $parameters";
        close STDOUT;
    }
    open STDOUT, '>&OLDOUT' or die "Can't restore stdout: $!";
    close OLDOUT or die "Can't close OLDOUT: $!";
}

no Moose;
1;

