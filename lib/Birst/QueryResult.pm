package Birst::QueryResult;
use 5.012;
use strict;
use warnings FATAL => 'all';
use Moose;
use SOAP::Lite;
use DateTime;

BEGIN {
    require DateTime::Format::Flexible;
}

*parse_datetime = \&DateTime::Format::Flexible::parse_datetime;

has 'row_count' => (
    is => 'ro',
    isa => 'Int',
    writer => '_row_count',
);

has 'more_rows' => (
    is => 'ro',
    isa => 'Bool',
    writer => '_more_rows',
    predicate => 'has_more',
);

has 'columns' => (
    is => 'ro',
    isa => 'ArrayRef',
    writer => '_columns',
);

has 'names' => (
    is => 'ro',
    isa => 'ArrayRef',
    writer => '_names',
);

has 'rows' => (
    is => 'ro',
    isa => 'ArrayRef',
    writer => '_rows',
);

has 'token' => (
    is => 'ro',
    isa => 'Str',
    writer => '_token',
);

has 'som' => (
    is => 'ro',
    isa => 'SOAP::SOM',
    writer => '_som',
);

around BUILDARGS => sub {
    my ($orig, $class) = (shift, shift);
    return $class->$orig(som => $_[0]);
};

sub BUILD {
    my $self = shift;

    my $raw_result = $self->som->result || return;
    my (@types, @columns, @display_names, @rows);
    if (ref $raw_result->{dataTypes}->{int} eq 'ARRAY') {
        @types = @{$raw_result->{dataTypes}->{int}};
    }
    else {
        @types = ($raw_result->{dataTypes}->{int});
    }
    if (ref $raw_result->{columnNames}->{string} eq 'ARRAY') {
        @columns = @{$raw_result->{columnNames}->{string}};
    }
    else {
        @columns = ($raw_result->{columnNames}->{string});
    }
    if (ref $raw_result->{displayNames}->{string}) {
        @display_names = @{$raw_result->{displayNames}->{string}};
    }
    else {
        @display_names = ($raw_result->{displayNames}->{string});
    }
    # Make sure we are consistent and always return an ARRAY ref for each row.
    if (ref $raw_result->{rows}->{ArrayOfString} eq 'ARRAY') {
        @rows = map { ref $_->{string} ? $_->{string} : [$_->{string}] } @{$raw_result->{rows}->{ArrayOfString}};
    }
    elsif (ref $raw_result->{rows}->{ArrayOfString} eq 'HASH') {
        my $row = $raw_result->{rows}->{ArrayOfString}->{string};
        @rows = ref $row ? $row : [$row];
    }

    $self->_row_count($raw_result->{numRowsReturned} || 0);
    $self->_more_rows($raw_result->{hasMoreRows} eq 'true');
    $self->_token($raw_result->{queryToken} || '');

# Do DateTime conversion
    for my $row (@rows) {
        for (0..$#types) {
            if ($types[$_] == 93) {
                $row->[$_] = parse_datetime($row->[$_]);
            }
        }
    }

    $self->_columns(\@columns);
    $self->_names(\@display_names);
    $self->_rows(\@rows);
    undef;
}

sub fetch {
    my $self = shift;
    return unless scalar @{$self->rows};
    $self->_row_count($self->row_count - 1);
    shift $self->rows;
}

1;
