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
use IO::Compress::Zip qw(zip $ZipError);
use File::Temp qw(:seekable);
use MIME::Base64 qw(encode_base64);
use Path::Tiny;
use DateTime;
use DateTime::Format::Flexible;

our $VERSION = '0.01';

my $ns = 'http://www.birst.com/';
my $xsd_datetime_format = '%Y-%m-%dT%H:%M:%S';

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
        upload_token => 0,
        publish_token => 0,
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
    # escape parens in space name
    $space_name =~ s/(\(|\))/\\$1/g;
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
    $self->_call("Logout");
    $self->{token} = 0;
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

sub clear_cache {
    my $self = shift;
    my $space_id = $self->{space_id} || die "No space id.";
    my $som = $self->_call('clearCacheInSpace',
                 SOAP::Data->name('spaceID')->value($space_id),
            );
    my $result = $som->valueof('//clearCacheInSpaceResponse/clearCacheInSpaceResult');
    $result eq 'true';
}

sub clear_dashboard_cache {
    my $self = shift;
    my $space_id = $self->{space_id} || die "No space id.";
    $self->_call('clearDashboardCache',
                 SOAP::Data->name('spaceID')->value($space_id),
            );
}

sub spaces {
    my $self = shift;
    my $som = $self->_call('listSpaces');
    defined $som ? $som->result->{UserSpace} : undef;
}

sub upload {
    my ($self, $filename) = (shift, shift);
    my $space_id = $self->{space_id} || die "No space id set.";
    
    my %opts = @_;
    my @opts = ();
    my @opt_list = qw(ConsolidateIdenticalStructures ColumnNamesInFirstRow
                      FilterLikelyNoDataRows LockDataSourceFormat
                      IgnoreQuotesNotAtStartOrEnd RowsToSkipAtStart RowsToSkipAtEnd
                      CharacterEncoding);
    for (@opts) {
        push @opts, SOAP::Data->name($_)->value($opts{$_}) if exists $opts{$_};
    }
    
    my $upload_filename = path($filename)->basename;
    my $fh;
    if ($opts{compression} == 1) {
        $upload_filename = path($upload_filename)->basename(qr/\.\w+$/) . '.zip';
        $fh = File::Temp->new;
        zip $filename => $fh, BinModeIn => 1 or die "compression failed: $ZipError";
    }
    else {
        $fh = path($filename)->readr_raw;
    }
    
    my $som = $self->_call('beginDataUpload',
                 SOAP::Data->name('spaceID')->value($space_id),
                 SOAP::Data->name('sourceName')->value($upload_filename),
            );
    
    my $upload_token = $som->valueof('//beginDataUploadResponse/beginDataUploadResult');
    
    if (@opts) {
        $self->_call('setDataUploadOptions',
                     SOAP::Data->name('dataUploadToken')->value($upload_token),
                     SOAP::Data->name('options')->value(\@opts),
                );
    }
    
    $fh->seek(0, SEEK_SET);
    my $data = undef;
    my $bytes = 0;
    my $chunk_size = exists $opts{chunk_size} ? $opts{chunk_size} : 65536;
    while ($bytes = read($fh, $data, $chunk_size)) {
        $data = encode_base64($data);
        $self->_call('uploadData',
                     SOAP::Data->name('dataUploadToken')->value($upload_token),
                     SOAP::Data->name('numBytes')->value($bytes),
                     SOAP::Data->name('data')->value($data),
                );
    }
    
    $self->_call('finishDataUpload',
                 SOAP::Data->name('dataUploadToken')->value($upload_token),
            );
    
    $self->{upload_token} = $upload_token;
    $self;
}

sub upload_status {
    my $self = shift;
    my $upload_token = $self->{upload_token} || die "no upload token";
    my $som = $self->_call('isDataUploadComplete',
                 SOAP::Data->name('dataUploadToken')->value($upload_token),
            );
    my $result = $som->valueof('//isDataUploadCompleteResponse/isDataUploadCompleteResult');
    if ($result eq 'false') {
        return 0;
    }
    $som = $self->_call('getDataUploadStatus',
                SOAP::Data->name('dataUploadToken')->value($upload_token),
            );
    $som->result || 1;
}

sub process_data {
    my $self = shift;
    my $space_id = $self->{space_id} || die "No space id set.";
    my %opts = @_;
    
    my $date;
    if (exists $opts{date}) {
        $date = DateTime::Format::Flexible->parse_datetime($opts{date})->strftime($xsd_datetime_format);
    }
    else {
        $date = DateTime->now->strftime($xsd_datetime_format);
    }
    
    my @subgroups = ();
    if (exists $opts{subgroups}) {
        if (ref $opts{subgroups} eq 'ARRAY') {
            @subgroups = map { SOAP::Data->name('string')->value($_) } @{$opts{subgroups}};
        }
        else {
            @subgroups = (SOAP::Data->name('string')->value($opts{subgroups}));
        }
    }
    
    my $som = $self->_call('publishData',
                 SOAP::Data->name('spaceID')->value($space_id),
                 SOAP::Data->name('subgroups')->value(\@subgroups),
                 SOAP::Data->name('date')->value($date),
            );
    $self->{publish_token} = $som->valueof('//publishDataResponse/publishDataResult');
}

sub process_data_status {
    my $self = shift;
    my $publish_token = $self->{publish_token} || die "No publishing token.";
    
    my $som = $self->_call('isPublishingComplete',
                 SOAP::Data->name('publishingToken')->value($publish_token),
            );
    my $result = $som->valueof('//isPublishingCompleteResponse/isPublishingCompleteResult');
    if ($result eq 'false') {
        return 0;
    }
    $som = $self->_call('getPublishingStatus',
                SOAP::Data->name('publishingToken')->value($publish_token),
            );
    $som->result || 1;
}

sub create_subject_area {
    my ($self, $name, $description) = (shift, shift, shift);
    my $space_id = $self->{space_id} || die "No space id.";
    my %opts = @_;
    
    my @groups = ();
    if (exists $opts{groups}) {
        if (ref $opts{groups} eq 'ARRAY') {
            @groups = map { SOAP::Data->name('string')->value($_) } @{$opts{groups}};
        }
        else {
            @groups = (SOAP::Data->name('string')->value($opts{groups}));
        }
    }
    
    $self->_call('createSubjectArea',
            SOAP::Data->name('spaceID')->value($space_id),
            SOAP::Data->name('name')->value($name),
            SOAP::Data->name('description')->value($description),
            SOAP::Data->name('groups')->value(\@groups),
        );
    $self;
}

sub delete_subject_area {
    my ($self, $name) = @_;
    my $space_id = $self->{space_id} || die "No space id.";
    
    $self->_call('deleteSubjectArea',
            SOAP::Data->name('spaceID')->value($space_id),
            SOAP::Data->name('name')->value($name),
        );
    $self;
}

sub create_directory {
    my ($self, $dir) = @_;
    my $space_id = $self->{space_id} || die "No space id.";
    my $directory = path($dir)->basename;
    my $parent = path($dir)->dirname;
    $parent =~ s/\/$//;
    
    my $som = $self->_call('checkAndCreateDirectory',
                SOAP::Data->name('spaceID')->value($space_id),
                SOAP::Data->name('parentDir')->value($parent),
                SOAP::Data->name('newDirectoryName')->value($directory),
            );
    my $result = $som->valueof('//checkAndCreateDirectoryResponse/checkAndCreateDirectoryResult');
    $result eq 'true';
}

sub releases {
    my $self = shift;
    my $som = $self->_call('listReleases');
    $som->result->{string};
}

sub get_engine_version {
    my $self = shift;
    my $space_id = $self->{space_id} || die "No space id.";
    my $som = $self->_call('getSpaceProcessEngineVersion',
                 SOAP::Data->name('spaceID')->value($space_id),
            );
    $som->valueof('//getSpaceProcessEngineVersionResponse/getSpaceProcessEngineVersionResult');
}

sub set_engine_version {
    my ($self, $version) = @_;
    my $space_id = $self->{space_id} || die "No space id.";
    my $som = $self->_call('setSpaceProcessEngineVersion',
                SOAP::Data->name('spaceID')->value($space_id),
                SOAP::Data->name('processingVersionName')->value($version),
            );
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
