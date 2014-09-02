package Birst::API;
use strict;
use warnings FATAL => 'all';

use DateTime;
use HTTP::Cookies;
use List::MoreUtils qw(none);
use Moose::Util::TypeConstraints;
use Moose;
use MooseX::Params::Validate;
use SOAP::Lite;
use Birst::Space;


BEGIN {
    require DateTime::Format::Flexible;
}

*parse_datetime = \&DateTime::Format::Flexible::parse_datetime;

our $VERSION = '0.01';

my  $ns = 'http://www.birst.com/';
our $xsd_datetime_format = '%Y-%m-%dT%H:%M:%S';

my @endpoints = (
    'https://app2101.bws.birst.com/CommandWebService.asmx?WSDL',
    'https://app2102.bws.birst.com/CommandWebService.asmx?WSDL',
    'https://app2103.bws.birst.com/CommandWebService.asmx?WSDL',
    'https://rc.birst.com/CommandWebService.asmx?WSDL',
);

subtype 'Birst::Version',
    as 'Str',
    where { /\d+\.\d+/ },
    message { "Version '$_' is not in a valid format" };

enum 'Birst::CopyType', [qw(copy replicate)];

has 'client' => (
    is => 'ro',
    isa => 'SOAP::Lite',
    writer => '_client',
);

has 'auth_token' => (
    is => 'ro',
    isa => 'Str',
    writer => '_auth_token',
    clearer => 'clear_auth_token',
    predicate => 'has_auth_token',
    init_arg => undef,
);

has 'job_token' => (
    is => 'ro',
    isa => 'Str',
    writer => '_job_token',
    clearer => 'clear_job_token',
    predicate => 'has_job_token',
    init_arg => undef,
);

sub BUILD {
    my ($self, $params) = (shift, shift);
    $self->_client(SOAP::Lite->new(proxy => $params->{endpoint})->default_ns($ns)->envprefix('soap'));
    $self->client->transport->proxy->cookie_jar(HTTP::Cookies->new);
    SOAP::Lite->import(+trace => 'all'), $self->client->readable(1)
        if defined $params->{debug} and $params->{debug} == 1;
}

sub _print_fault {
    my $self = shift;
    my ($message) = pos_validated_list(\@_, { isa => 'Str' });
    # Try and make fault messages less cryptic
    die "Logical query syntax is incorrect." if $message =~ /Object reference not set to an instance of an object./;
    die $message . "\n";
}

sub _call {
    my $self = shift;
    my ($method, $data) = pos_validated_list(\@_,
        { isa => 'Str' },
        { isa => 'ArrayRef[SOAP::Data]', optional => 1 },
    );
    my $som = $self->client->on_action(sub {"${ns}${method}"})
        ->call($method => SOAP::Data->name('token')->value($self->auth_token),
            ref $data eq 'ARRAY' ? @{ $data } : ());
    die $self->_print_fault($som->faultstring) if $som->fault;
    return $som;
}

sub get_endpoints { return \@endpoints }

sub get_space_id {
    my $self = shift;
    my ($space_name) = pos_validated_list(\@_, { isa => 'Str' });
    my $spaces = $self->spaces;
    die "Unable to retrieve list of spaces\n" unless $spaces;
    my @ids = map { $_->{id} } grep { $_->{name} =~ /\Q$space_name\E/ } @$spaces;
    die "Could not find space named '$space_name'\n" if not @ids;
    return $ids[0];
}

sub login {
    my $self = shift;
    my ($username, $password) = pos_validated_list(\@_,
        { isa => 'Str' },
        { isa => 'Str' },
    );
    $self->client->on_action(sub { "${ns}Login" });
    my $som = $self->client->call('Login',
        SOAP::Data->name('username')->value($username),
        SOAP::Data->name('password')->value($password),
    );
    $som->faultstring if $som->fault;
    $self->_auth_token($som->valueof('//LoginResponse/LoginResult'));
}

sub logout {
    my $self = shift;
    $self->client->on_action(sub { "${ns}Logout" });
    $self->_call('Logout');
    $self->clear_auth_token;
}

sub space {
    my $self = shift;
    my ($id) = pos_validated_list(\@_, { isa => 'Birst::SpaceId' });
    return Birst::Space->new(client => $self, id => $id);
}

sub copy_file {
    my $self = shift;
    my ($from_space_id, $from_filename, $to_space_id, $to_filename) =
        pos_validated_list([shift, shift, shift, shift],
            { isa => 'Birst::SpaceId' },
            { isa => 'Str' },
            { isa => 'Birst::SpaceId' },
            { isa => 'Str' },
        );

    my $should_overwrite = validated_list(\@_,
        overwrite => { isa => 'Bool', default => 0 });
    my $overwrite = $should_overwrite ? 'true' : 'false';
    $self->_call('copyFile',
        [
            SOAP::Data->name('fromSpaceID')->value($from_space_id),
            SOAP::Data->name('fromFileOrDir')->value($from_filename),
            SOAP::Data->name('toSpaceID')->value($to_space_id),
            SOAP::Data->name('toFileOrDir')->value($to_filename),
            SOAP::Data->name('overwrite')->value($overwrite),
        ],
    );
}

sub copy_file_or_dir {
    my $self = shift;
    my ($from_space_id, $filename, $to_space_id, $to_dir) =
        pos_validated_list(\@_,
            { isa => 'Birst::SpaceId' },
            { isa => 'Str' },
            { isa => 'Birst::SpaceId' },
            { isa => 'Str' },
        );

    my $som = $self->_call('copyFileOrDirectory',
        [
            SOAP::Data->name('fromSpaceID')->value($from_space_id),
            SOAP::Data->name('fileOrDir')->value($filename),
            SOAP::Data->name('toSpaceID')->value($to_space_id),
            SOAP::Data->name('toDir')->value($to_dir)
        ],
    );
    $som->valueof('//copyFileOrDirectoryResponse/copyFileOrDirectoryResult');
}

sub spaces {
    my $self   = shift;
    my $som    = $self->_call('listSpaces');
    my $result = undef;
    if (defined $som) {
        if (ref $som->result->{UserSpace} eq 'ARRAY') {
            $result = $som->result->{UserSpace};
        }
        elsif (ref $som->result->{UserSpace} eq 'HASH') {
            $result = [$som->result->{UserSpace}];
        }
    }
    return unless defined $result;
    if (wantarray) {
        return @{ $result };
    }
    else {
        return $result;
    }
}

sub releases {
    my $som = shift->_call('listReleases');
    return $som->result->{string};
}

sub _copyopt {
    my ($self, $arg) = @_;
    local $_ = $arg;
    tr/_/-/;
    return 'DrillMaps.xml' if /drill(-|)map/;
    return 'CustomGeoMaps.xml' if /geo(-|)map/;
    return 'spacesettings.xml' if /space(-|)setting/;
    return 'SavedExpressions.xml' if /saved(-|)expression/;
    if (/datastore-\w+-/) {
        s/-(\w+)$/_$1/;
    }
    return $_;
}

sub _buildcopyopts {
    my $self = shift;
    my %opts = @_;
    my @copy_options = ();
    for (keys %opts) {
        if (not ref $opts{$_}) {
            if ($opts{$_} == 1) {
                push @copy_options, $self->_copyopt($_);
            }
            else {
                push @copy_options, join(':', $self->_copyopt($_), $opts{$_});
            }
        }
        elsif (ref $opts{$_} eq 'ARRAY') {
            # add option in form <opt>:<item1>,<item2>...
            push @copy_options, join(':', $self->_copyopt($_), join(',', @{$opts{$_}}));
        }
    }
    return join(';', @copy_options);
}

sub _copyspace {
    my ($self, $from, $to, $type) = validated_list(
        [shift, shift, shift, shift],
        from => { isa => 'Str' },
        to   => { isa => 'Str' },
        type => { isa => 'Birst::CopyType' },
    );
    my $from_id = $self->get_space_id_by_name($from);
    my $to_id   = $self->get_space_id_by_name($to);
    my $options = $self->_buildcopyopts(@_);
    my $som     = $self->_call('copySpace',
        [
            SOAP::Data->name('spFromID')->value($from_id),
            SOAP::Data->name('spToID')->value($to_id),
            SOAP::Data->name('mode')->value($type),
            SOAP::Data->name('options')->value($options),
        ],
    );
    $self->_job_token($som->valueof('//copySpaceResponse/copySpaceResult'));
}

sub copy_space {
    $_[0]->_copyspace(type => 'copy', @_);
}

sub copy_space_sync {
    my $self = shift;
    $self->copy_space(@_);
    $self->_wait_for_status(\&job_status);
}

sub replicate_space {
    $_[0]->_copyspace('replicate', @_);
}

sub replicate_space_sync {
    my $self = shift;
    $self->replicate_space(@_);
    $self->_wait_for_status(\&job_status);
}

sub delete_space {
    my $self = shift;
    my ($space) = pos_validated_list(\@_, { isa => 'Birst::SpaceId' });
    my $som = $self->_call('deleteSpace',
        [ SOAP::Data->name('spaceId')->value($self->space_id) ],
    );
    $self->_job_token($som->valueof('//deleteSpaceResponse/deleteSpaceResult'));
}

sub delete_space_sync {
    my $self = shift;
    $self->delete_space(@_);
    $self->_wait_for_status(\&job_status);
}

sub _wait_for_status {
    my $self = shift;
    my ($status_method) = pos_validated_list(\@_, { isa => 'CodeRef' });
    
    my $result = 0;
    while (not $result) {
        $result = $status_method->($self);
        sleep 10 unless $result;
    }
    return $result;
}

sub job_status {
    my $self = shift;
    my $som = $self->_call('isJobComplete',
        [ SOAP::Data->name('jobToken')->value($self->job_token) ],
    );
    my $result = $som->valueof('//isJobCompleteResponse/isJobCompleteResult');
    return 0 if ($result eq 'false');
    $som = $self->_call('getJobStatus',
        [ SOAP::Data->name('jobToken')->value($self->job_token) ],
    );
    $self->clear_job_token;
    return ($som->result || 1);
}

sub swap_space_contents {
    my $self = shift;
    my ($space_from, $space_to) = pos_validated_list(\@_,
        { isa => 'Birst::SpaceId' },
        { isa => 'Birst::SpaceId' },
    );
    my $space_from_id = $self->get_space_id_by_name($space_from);
    my $space_to_id   = $self->get_space_id_by_name($space_to);
    my $som = $self->_call('swapSpaceContents',
        [
            SOAP::Data->name('sp1ID')->value($space_from_id),
            SOAP::Data->name('sp2ID')->value($space_to_id),
        ],
    );
    $self->_job_token($som->valueof('//swapSpaceContentsResponse/swapSpaceContentsResult'));
}

sub swap_space_contents_sync {
    my $self = shift;
    $self->swap_space_contents(@_);
    $self->_wait_for_status(\&job_status);
}

sub allow_ip_cidr {
    my $self = shift;
    my ($ip) = pos_validated_list([shift], { isa => 'CIDR' });
    my $user = validated_list(\@_,
        user => { isa => 'Str', optional => 1 });
    if (defined $user) {
        $self->_call('addAllowedIp',
            [
                SOAP::Data->name('userName')->value($user),
                SOAP::Data->name('ip')->value($ip),
            ],
        );
    }
    else {
        $self->_call('addAllowedIPAddrForAccount',
            [ SOAP::Data->name('ip')->value($ip) ],
        );
    }
}

sub add_openid {
    my $self = shift;
    my ($user, $openid) = pos_validated_list(\@_,
        { isa => 'Str' },
        { isa => 'Str' },
    );
    $self->_call('addOpenID',
        [
            SOAP::Data->name('userName')->value($user),
            SOAP::Data->name('openID')->value($openid),
        ],
    );
}

sub add_proxy_user {
    my $self = shift;
    my ($user, $proxyuser, $expire) = pos_validated_list(\@_,
        { isa => 'Str' },
        { isa => 'Str' },
        { isa => 'Str' },
    );
    my $xsd_expire = parse_datetime($expire)->strftime($xsd_datetime_format);
    $self->_call('addProxyUser',
        [
            SOAP::Data->name('userName')->value($user),
            SOAP::Data->name('proxyUserName')->value($proxyuser),
            SOAP::Data->name('expiration')->value($xsd_expire),
        ],
    );
}

sub add_user {
    my $self = shift;
    my ($user) = pos_validated_list([shift], { isa => 'Str' });
    my %opts = validated_hash(\@_,
        email    => { isa => 'Str', optional => 1 },
        password => { isa => 'Str', optional => 1 },
    );
    my $params = join(' ', map { $_ . "=" . $opts{$_} } \
                      grep { defined $opts{$_} } qw(email password));
    $self->_call('addUser',
        [
            SOAP::Data->name('userName')->value($user),
            SOAP::Data->name('additionalParams')->value($params),
        ],
    );
}

sub copy_catalog_dir {
    my $self = shift;
    my ($space_from, $space_to, $dir) = pos_validated_list(\@_,
        { isa => 'Birst::SpaceId' },
        { isa => 'Birst::SpaceId' },
        { isa => 'Str' },
    );
    my $som = $self->_call('copyCatalogDirectory',
        [
            SOAP::Data->name('spFromID')->value($space_from),
            SOAP::Data->name('spToID')->value($space_to),
            SOAP::Data->name('directoryName')->value($dir),
        ],
    );
    $som->valueof('//copyCatalogDirectoryResponse/copyCatalogDirectoryResult') eq 'true';
}

sub copy_subject_area {
    my $self = shift;
    my ($space_from, $space_to, $area) = pos_validated_list(\@_,
        { isa => 'Birst::SpaceId' },
        { isa => 'Birst::SpaceId' },
        { isa => 'Str' },
    );
    $self->_call('copyCustomSubjectArea',
        [
            SOAP::Data->name('fromSpaceId')->value($space_from),
            SOAP::Data->name('toSpaceId')->value($space_to),
            SOAP::Data->name('customSubjectAreaName')->value($area),
        ],
    );
}

sub create_space {
    my $self  = shift;
    my ($space) = pos_validated_list([shift], { isa => 'Birst::SpaceId' });
    my %opts  = validated_hash(\@_,
        comment   => { isa => 'Str',  default  => '' },
        automatic => { isa => 'Bool', default  => 0  },
        schema    => { isa => 'Str',  optional => 1  },
    );
    my $automatic = $opts{automatic} ? 'true' : 'false';
    my @params = (
        SOAP::Data->name('spaceName')->value($space),
        SOAP::Data->name('comments')->value($opts{comment}),
        SOAP::Data->name('automatic')->value($automatic),
    );
    my $som;
    if (defined $opts{schema}) {
        push @params, SOAP::Data->name('schemaName')->value($opts{schema});
        $som = $self->_call('createNewSpaceUsingSchema', \@params);
    }
    else {
        $som = $self->_call('createNewSpace', \@params);
    }
    return ($som->valueof('//createNewSpaceResponse/createNewSpaceResult') eq 'true');
}

sub delete_user {
    my $self = shift;
    my ($user) = pos_validated_list(\@_, { isa => 'Str' });
    $self->_call('deleteUser',
        [ SOAP::Data->name('userName')->value($user) ],
    );
}

sub enable_account {
    my $self = shift;
    my ($account_id) = pos_validated_list(\@_, { isa => 'Str' });
    $self->_call('enableAccount',
        [
            SOAP::Data->name('accountID')->value($account_id),
            SOAP::Data->name('enable')->value('true'),
        ],
    );
}

sub disable_account {
    my $self = shift;
    my ($account_id) = pos_validated_list(\@_, { isa => 'Str' });
    $self->_call('enableAccount',
        [
            SOAP::Data->name('accountID')->value($account_id),
            SOAP::Data->name('enable')->value('false'),
        ],
    );
}

sub enable_user {
    my $self = shift;
    my ($user) = pos_validated_list(\@_, { isa => 'Str' });
    $self->_call('enableUser',
        [
            SOAP::Data->name('userName')->value($user),
            SOAP::Data->name('enable')->value('true'),
        ],
    );
}

sub disable_user {
    my $self = shift;
    my ($user) = pos_validated_list(\@_, { isa => 'Str' });
    $self->_call('enableUser',
        [
            SOAP::Data->name('userName')->value($user),
            SOAP::Data->name('enable')->value('false'),
        ],
    );
}

=encoding utf8

=head1 NAME

Birst::API - Client library for the Birst business intelligence platform.

=head1 SYNOPSIS

    use Birst::API;
    use Data::Dump;

    my $endpoint = Birst::API::get_endpoints()->[0];
    my $client = Birst::API->new(endpoint => $endpoint);
    my $id = $client->get_space_id('My Space');
    my $space = $client->space($id);
    my $result = $space->query($statement);
    for (@{ $result->columns }) { print $_ }
    print "\n";
    while ($result->fetch_row) { dd $_ }
    $client->logout;

=head1 DESCRIPTION

This package provides access to the Birst SOAP API. It also contains some helper methods to make working
with the API simpler.

=head1 METHODS

=head2 login

    my $client = Birst::API->new(endpoint => $endpoint);
    $client->login($username, $password);

Login will authenticate user credentials with Birst. The login token is stored internally
and used for all subsequent API calls.

=head2 logout

    $client->logout;

Logs out current user from Birst and clears the login token.

=head2 spaces

    $client->spaces;

=head2 get_space_id

    my $id = $client->get_space_id('My Space Name');

=head2 query

    my $statment = "SELECT [Quantity],[Sales] FROM [ALL]";
    my $result = $client->query($statement);

Submit a logical query to Birst.

=head2 copy_file and copy_file_or_dir

    $client->copy_file('From Space', 'My Report', 'To Space', 'My Report', 1);

=head2 clear_cache

    $client->clear_cache;

Clear the query cache.

=head2 clear_dashboard_cache

    $client->clear_dashboard_cache;

Clear cache for dashboard report queries.

=head2 upload, upload_status, upload_sync

    $client->upload('myfile.csv', compression => 1, ColumnNamesInFirstRow => 'true');

    while (not $client->upload_status) {
        # do something
    }

    $client->upload_sync('myfile.csv');

Upload a data source file.

=head2 process_data, process_data_status, process_data_sync

    $client->process_data(subgroups => ['group1', 'group2']);
   
    while (not $client->process_data_status) {
        # do something
    }

    $client->process_data_sync(subgroups => 'groupname', date => '2014-06-01');

Process data for sources.
If no subgroups are specified then all groups are processed.
If date is not specified then the current date is used.

=head2 sources

    my @sources = @{$client->sources};

Get list of all sources in the current space.

=head2 source_details

    my $details = $client->source_details('my source');

Get data structure with detailed information about a data source.

=head2 delete_all_data, delete_all_data_sync

    $client->delete_all_data;

    while (not $client->job_status) {
        # do something
    }

    $client->delete_all_data_sync;

Delete all data in the current space.

=head2 delete_last_data, delete_last_data_sync

    $client->delete_last_data;

    while (not $client->job_status) {
        # do something
    }

    $client->delete_last_data_sync;

Delete last load of data from the current space.

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
