package Bio::VertRes::Permissions::ParseFileOfDirectories;

# ABSTRACT:  Parse a file of directories and return an array

=head1 SYNOPSIS

Parse a file of directories and return an array with directories which exist
   use Bio::VertRes::Permissions::ParseFileOfDirectories;
   
   my $obj = Bio::VertRes::Permissions::ParseFileOfDirectories->new(input_file => 'inputfile');
   $obj->directories();

=cut

use Moose;
with 'Bio::VertRes::Permissions::LoggerRole';

has 'input_file'   => ( is => 'ro', isa => 'Str', required => 1 );
has 'directories'  => ( is => 'ro', isa => 'ArrayRef', lazy => 1, builder => '_build_directories' );

sub _build_directories
{
	my ($self) = @_;

    unless(-e $self->input_file)
	{
		$self->logger->error( "Input file of directories doesnt exist: ". $self->input_file );	
		die();
	}
    
	my @directories;
	open(my $fh, $self->input_file);
	while(<$fh>)
	{
		chomp;
		my $directory = $_;
		
		next if($directory =~ /^#/ || $directory eq "");
		
		if(! -d $directory)
		{
			$self->logger->warn( "Cannot access directory (skipping): ". $directory );
		}
		else
		{
			push(@directories, $directory);
		}
	}
	$self->logger->info( "Number of directories read in from file: ". @directories );
	return \@directories;
}

no Moose;
__PACKAGE__->meta->make_immutable;

1;
