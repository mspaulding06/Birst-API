package Birst::Filter;
use SOAP::Lite;

sub new {
    my $class = shift;
    my %opts = @_;
    my $self = bless {
        _filter_type      => $opts{filter_type} || 'data',
        _op               => $opts{op},
        _multiselect_type => $opts{multiselect_type},
        _column           => $opts{column},
        _values           => ref $opts{values} eq 'ARRAY' ? $opts{values} : [],
    }, $class;
}

sub filter_type {
    my ($thing, $filter) = @_;
    my $self = ref $thing ? $thing : $thing->new;
    $self->{_filter_type} = $filter;
    $self;
}

sub op {
    my ($thing, $op) = @_;
    my $self = ref $thing ? $thing : $thing->new;
    $self->{_op} = $op;
    $self;
}

sub multiselect_type {
    my ($thing, $multiselect) = @_;
    my $self = ref $thing ? $thing : $thing->new;
    $self->{_multiselect_type} = $multiselect;
    $self;
}

sub column {
    my ($thing, $column) = @_;
    my $self = ref $thing ? $thing : $thing->new;
    $self->{_column} = $column;
    $self;
}

sub value {
    my ($thing, $value) = @_;
    my $self = ref $thing ? $thing : $thing->new;
    push @{$self->{_values}}, $value;
    $self;
}

sub _soapify_values {
    my $self = shift;
    my @values = ();
    for (@{$self->{_values}}) {
        push @values, SOAP::Data->name('selectedValue')->value($_);
    }
    \@values;
}

sub soapify {
    my $thing = shift;
    my $self = ref $thing ? $thing : $thing->new;
    SOAP::Data->name('Filter')->value([
        SOAP::Data->name('Operator')->value($self->{_op}),
        SOAP::Data->name('multiSelectType')->value($self->{_multiselect_type}),
        SOAP::Data->name('FilterType')->value($self->{_filter_type}),
        SOAP::Data->name('ParameterName')->value($self->{_column}),
        SOAP::Data->name('selectedValues')->value($self->_soapify_values),
    ]);
}

1;