package Birst::Filter;
use Moose;
use Moose::Util::TypeConstraints;
use SOAP::Lite;


enum 'Birst::FilterType'      => [qw(Data Display Set-based)];
enum 'Birst::MultiselectType' => [qw(OR AND)];
enum 'Birst::Operation'       => ['>', '<', '>=', '<=', '<>', 'LIKE', 'NOT LIKE', 'IS NULL', 'IS NOT NULL'];

subtype 'Birst::SelectedValues' => as 'ArrayRef[Str]';
coerce  'Birst::SelectedValues' => from 'Str' => via { [$_] };

has 'filter_type' => (
    is => 'rw',
    isa => 'Birst::FilterType',
    required => 1,
    default => 'Data',
);

has 'op' => (
    is => 'rw',
    isa => 'Birst::Operation',
    required => 1,
);

has 'multiselect_type' => (
    is => 'rw',
    isa => 'Birst::MultiselectType',
    required => 1,
    default => 'OR',
);

has 'column' => (
    is => 'rw',
    isa => 'Str',
    required => 1,
);

has 'values' => (
    is => 'rw',
    isa => 'Birst::SelectedValues',
    required => 1,
    coerce => 1,
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
