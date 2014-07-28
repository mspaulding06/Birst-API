package Birst::Filter;
use Moose;
use Moose::Util::TypeConstraints;
use SOAP::Lite;

enum 'FilterType'      => [qw(Data Display Set-based)];
enum 'MultiselectType' => [qw(OR AND)];
enum 'Operation'       => ['>', '<', '>=', '<=', '<>', 'LIKE', 'NOT LIKE', 'IS NULL', 'IS NOT NULL'];

has 'filter_type' => (
    is => 'rw',
    isa => 'FilterType',
    required => 1,
    default => 'Data',
    lazy => 1,
);

has 'op' => (
    is => 'rw',
    isa => 'Operation',
    required => 1,
);

has 'multiselect_type' => (
    is => 'rw',
    isa => 'MultiselectType',
    required => 1,
    default => 'OR',
    lazy => 1,
);

has 'column' => (
    is => 'rw',
    isa => 'Str',
    required => 1,
);

has 'values' => (
    is => 'rw',
    isa => 'ArrayRef',
    required => 1,
    default => sub { [] },
);

sub _soapify_values {
    my $self = shift;
    my @values = ();
    for (@{$self->values}) {
        push @values, SOAP::Data->name('selectedValue')->value($_);
    }
    \@values;
}

sub soapify {
    my $self = shift;
    SOAP::Data->name('Filter')->value([
        SOAP::Data->name('Operator')->value($self->op),
        SOAP::Data->name('multiSelectType')->value($self->multiselect_type),
        SOAP::Data->name('FilterType')->value($self->filter_type),
        SOAP::Data->name('ParameterName')->value($self->column),
        SOAP::Data->name('selectedValues')->value($self->_soapify_values),
    ]);
}

1;
