package Birst::QueryResult;
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

has 'client' => (
    is => 'ro',
    isa => 'Birst::API',
    writer => '_client',
);

around BUILDARGS => sub {
    my ($orig, $class) = (shift, shift);
    return $class->$orig(client => $_[0], som => $_[1]);
};

sub BUILD {
    my $self = shift;
    my $raw_result = $self->som->result || return;
    $self->_parse_soap_response($raw_result);
}

sub _parse_soap_response {
    my ($self, $raw_result) = @_;
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
    if (defined $raw_result->{hasMoreRows}) {
        $self->_more_rows($raw_result->{hasMoreRows} eq 'true');
    }
    else {
        $self->_more_rows(0);
    }
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
}

sub _next_row {
    my $self = shift;
    return unless @{ $self->rows };
    $self->_row_count($self->row_count - 1);
    return (shift @{ $self->rows });
}

sub fetch_row {
    my $self = shift;
    my $row = $self->_next_row;
    if (not ref $row and $self->more_rows) {
        my $som = $self->client->_call('queryMore',
            [ SOAP::Data->name('queryToken')->value($self->token) ],
        );
        $self->_parse_soap_response($som);
        $row = $self->_next_row;
    }
    elsif (not ref $row) {
        return;
    }
    return ($_ = $row);
}

1;
