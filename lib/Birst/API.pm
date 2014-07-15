package Birst::QueryResult;
use strict;
use warnings FATAL => 'all';
use SOAP::Lite;
use DateTime;
use DateTime::Format::Flexible;

sub new {
    my ($class, $som) = @_;
    my $self = bless {
        row_count => 0,
        more_rows => 0,
        columns   => [],
        names     => [],
        rows      => [],
    }, $class;
    $self->_process_result($som);
    $self;
}

sub fetch {
    my $self = shift;
    return unless @{$self->{rows}};
    $self->{row_count}--;
    shift @{$self->{rows}};
}

sub has_more { $_[0]->{more_rows} }

sub token { $_[0]->{token} }

sub _process_result {
    my ($self, $som) = @_;
    my $raw_result = $som->result || return;
    my @types = @{$raw_result->{dataTypes}->{int}};
    my @columns = @{$raw_result->{columnNames}->{string}};
    my @display_names = @{$raw_result->{displayNames}->{string}};
    my @rows = ();
    if (ref $raw_result->{rows}->{ArrayOfString} eq 'ARRAY') {
        @rows = map { $_->{string} } @{$raw_result->{rows}->{ArrayOfString}};
    }
    elsif (ref $raw_result->{rows}->{ArrayOfString} eq 'HASH') {
        @rows = $raw_result->{rows}->{ArrayOfString}->{string};
    }

    $self->{row_count} = 0 + $raw_result->{numRowsReturned};
    $self->{more_rows} = $raw_result->{hasMoreRows} eq 'true' ? 1 : 0;
    $self->{token} = $raw_result->{queryToken};

# Do DateTime conversion
    for my $row (@rows) {
        for (0..$#types) {
            if ($types[$_] == 93) {
                $row->[$_] = DateTime::Format::Flexible->parse_datetime($row->[$_]);
            }
        }
    }

    $self->{columns} = \@columns;
    $self->{names}   = \@display_names;
    $self->{rows}    = \@rows;
    undef;
}

package Birst::API;
use strict;
use warnings FATAL => 'all';
use SOAP::Lite;
use HTTP::Cookies;

our $VERSION = '0.01';

my $ns = 'http://www.birst.com/';

my %wsdl = (
        rc => 'https://rc.birst.com/CommandWebService.asmx?WSDL',
        5.11 => 'https://app2103.bws.birst.com/CommandWebService.asmx?WSDL',
        5.12 => 'https://app2101.bws.birst.com/CommandWebService.asmx?WSDL',
        5.13 => 'https://app2102.bws.birst.com/CommandWebService.asmx?WSDL',
        );

sub new {
    my $class = shift;
    my %opts = @_;

    $opts{version} = 5.13 unless defined $opts{version};

    my $uri = $wsdl{$opts{version}};
    my $client = SOAP::Lite->new(proxy => $uri)->default_ns($ns)->envprefix('soap');
# TODO: Figure out why cookie_jar param does not work on SOAP::Lite
    $client->transport->proxy->cookie_jar(HTTP::Cookies->new);

    SOAP::Lite->import(+trace => 'all') if defined $opts{debug} and $opts{debug} == 1;

    bless {
        client => $client,
        token => 0,
        query_result => undef,
        query_token => 0,
        parse_datetime => 1,
    }, $class;
}

sub _call {
    my ($self, $method) = (shift, shift);
    my $som = $self->{client}->on_action(sub {"${ns}${method}"})
        ->call($method => SOAP::Data->name('token')->value($self->{token}), @_);
    die $som->faultstring if $som->fault;
    $som;
}

sub set_space_id {
    my ($self, $space_id) = @_;
    $self->{space_id} = $space_id;
    $self;
}

sub set_space_by_name {
    my ($self, $space_name) = @_;
    $self->set_space_id($self->get_space_id_by_name($space_name));
}

sub get_space_id_by_name {
    my ($self, $space_name) = @_;
    my $spaces = $self->spaces;
    die "Unable to receive list of spaces" unless $spaces;
    my @ids = map { $_->{id} } grep { $_->{name} =~ /$space_name/ } @$spaces;
    die "Could not find space named '$space_name'" if not @ids;
    $ids[0];
}

sub login {
    my ($self, $username, $password) = @_;
    $self->{client}->on_action(sub { "${ns}Login" });
    my $som = $self->{client}->call("Login",
            SOAP::Data->name('username')->value($username),
            SOAP::Data->name('password')->value($password),
            );
    $som->faultstring if $som->fault;
    $self->{token} = $som->valueof('//LoginResponse/LoginResult');
    $self;
}

sub logout {
    my $self = shift;
    $self->{client}->on_action(sub { "${ns}Logout" });
    my $som = $self->{client}->call("Logout");
    $self->{token} = 0;
    $som->faultstring if $som->fault;
    $self;
}

sub copy_file {
    my ($self, $from_space_id, $from_filename, $to_space_id, $to_filename, $overwrite) = @_;
    $self->_call('copyFile',
            SOAP::Data->name('fromSpaceID')->value($from_space_id),
            SOAP::Data->name('fromFileOrDir')->value($from_filename),
            SOAP::Data->name('toSpaceID')->value($to_space_id),
            SOAP::Data->name('toFileOrDir')->value($to_filename),
            SOAP::Data->name('overwrite')->value($overwrite),
            );
}

sub copy_file_or_dir {
    my ($self, $from_space_id, $filename, $to_space_id, $to_dir) = @_;
    my $som = $self->_call('copyFileOrDirectory',
            SOAP::Data->name('fromSpaceID')->value($from_space_id),
            SOAP::Data->name('fileOrDir')->value($filename),
            SOAP::Data->name('toSpaceID')->value($to_space_id),
            SOAP::Data->name('toDir')->value($to_dir),
            );
    $som->valueof('//copyFileOrDirectoryResponse/copyFileOrDirectoryResult');
}

sub query {
    my ($self, $query) = @_;
    my $space_id = $self->{space_id} || die "No space id set.";
    my $som = $self->_call('executeQueryInSpace',
            SOAP::Data->name('query')->value($query),
            SOAP::Data->name('spaceID')->value($space_id),
            );
    $self->{query_result} = Birst::QueryResult->new($som);
    $self->{query_token} = $self->{query_result}->token;
    $self;
}

sub fetch {
    my $self = shift;
    return unless $self->{query_result};
    my $result = $self->{query_result};
    my $row = $result->fetch;
    if (not ref $row and $result->has_more) {
        print "Fetching more rows\n";
        my $som = $self->_call('queryMore',
                SOAP::Data->name('queryToken')->value($self->{query_token}),
                );
        $self->{query_result} = Birst::QueryResult->new($som);
        $row = $self->{query_result}->fetch;
    }
    elsif (not ref $row) {
        $self->{query_result} = undef;
    }
    $_ = $row;
}

sub spaces {
    my $self = shift;
    my $som = $self->_call('listSpaces');
    defined $som ? $som->result->{UserSpace} : undef;
}

=encoding utf8

=head1 NAME

Birst::API - Wrapper for the Birst Data Warehouse SOAP API.

=head1 SYNOPSIS

    my $api = Birst::API(version => 5.13);
    $api->login($username, $password);
    $api->query($statement);

    while ($api->fetch) {
    # do something with query row
    }

    $api->logout;

=head1 DESCRIPTION

This package provides access to the Birst SOAP API. It also contains some helper methods to make working
with the API simpler.

=head1 METHODS

=head2 login

    my $client = Birst::API->new(version => 5.13);
    $client->login($username, $password);

Login will authenticate user credentials with Birst. The login token is stored internally
and used for all subsequent API calls.

=head2 logout

    $client->logout;

Logs out current user from Birst and clears the login token.

=head2 query

    my $statment = "SELECT [Quantity],[Sales] FROM [ALL]";
    $client->query($statement);

Submit a logical query to Birst.

=head2 fetch

    my @row = @{$client->fetch};

    OR

    while ($client->fetch) {
        print Dumper $_;
    }

Retrieve a single row from the query results. Returns L<undef> if there are no more rows.

=head2 copy_file and copy_file_or_dir

    $client->copy_file('From Space', 'My Report', 'To Space', 'My Report', 1);

=head1 AUTHOR

Matt Spaulding, C<< <mspaulding06 at gmail.com> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-birst-api at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Birst-API>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Birst::API


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Birst-API>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Birst-API>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Birst-API>

=item * Search CPAN

L<http://search.cpan.org/dist/Birst-API/>

=back


=head1 LICENSE AND COPYRIGHT

Copyright 2014 Matt Spaulding.

This program is free software; you can redistribute it and/or modify it
under the terms of the the Artistic License (2.0). You may obtain a
copy of the full license at:

L<http://www.perlfoundation.org/artistic_license_2_0>

Any use, modification, and distribution of the Standard or Modified
Versions is governed by this Artistic License. By using, modifying or
distributing the Package, you accept this license. Do not use, modify,
or distribute the Package, if you do not accept this license.

If your Modified Version has been derived from a Modified Version made
by someone other than you, you are nevertheless required to ensure that
your Modified Version complies with the requirements of this license.

This license does not grant you the right to use any trademark, service
mark, tradename, or logo of the Copyright Holder.

This license includes the non-exclusive, worldwide, free-of-charge
patent license to make, have made, use, offer to sell, sell, import and
otherwise transfer the Package with respect to any patent claims
licensable by the Copyright Holder that are necessarily infringed by the
Package. If you institute patent litigation (including a cross-claim or
counterclaim) against any party alleging that the Package constitutes
direct or contributory patent infringement, then this Artistic License
to you shall terminate on the date that such litigation is filed.

Disclaimer of Warranty: THE PACKAGE IS PROVIDED BY THE COPYRIGHT HOLDER
AND CONTRIBUTORS "AS IS' AND WITHOUT ANY EXPRESS OR IMPLIED WARRANTIES.
THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE, OR NON-INFRINGEMENT ARE DISCLAIMED TO THE EXTENT PERMITTED BY
YOUR LOCAL LAW. UNLESS REQUIRED BY LAW, NO COPYRIGHT HOLDER OR
CONTRIBUTOR WILL BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, OR
CONSEQUENTIAL DAMAGES ARISING IN ANY WAY OUT OF THE USE OF THE PACKAGE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


=cut

1; # End of Birst::API
