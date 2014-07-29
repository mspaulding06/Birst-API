package Birst::API;
use 5.012;
use strict;
use warnings FATAL => 'all';
use SOAP::Lite;
use HTTP::Cookies;
use IO::Compress::Zip qw(zip $ZipError);
use File::Temp qw(:seekable);
use MIME::Base64 qw(encode_base64 decode_base64);
use Path::Tiny;
use DateTime;
use List::MoreUtils qw(none);

BEGIN {
    require Birst::Filter;
    require DateTime::Format::Flexible;
}

*parse_datetime = \&DateTime::Format::Flexible::parse_datetime;

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

    SOAP::Lite->import(+trace => 'all'), $client->readable(1)
        if defined $opts{debug} and $opts{debug} == 1;

    bless {
        client         => $client,
        token          => 0,
        query_result   => undef,
        query_token    => 0,
        upload_token   => 0,
        publish_token  => 0,
        job_token      => 0,
        parse_datetime => 1,
    }, $class;
}

sub _print_fault {
    my ($self, $message) = @_;
    # Try and make fault messages less cryptic
    die "Logical query syntax is incorrect." if $message =~ /Object reference not set to an instance of an object./;
    die $message;
}

sub _call {
    my ($self, $method) = (shift, shift);
    my $som = $self->{client}->on_action(sub {"${ns}${method}"})
        ->call($method => SOAP::Data->name('token')->value($self->{token}), @_);
    die $self->_print_fault($som->faultstring) if $som->fault;
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
    my ($self, $from_space_id, $from_filename, $to_space_id, $to_filename) = @_;
    my %opts = @_;
    my $overwrite = $opts{overwrite} == 1 ? 'true' : 'false';
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
    require Birst::QueryResult;
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
	if (defined $som) {
		if (ref $som->result->{UserSpace} eq 'ARRAY') {
			return $_ = $som->result->{UserSpace};
		}
		elsif (ref $som->result->{UserSpace} eq 'HASH') {
			return $_ = [$som->result->{UserSpace}];
		}
	}
	undef;
}

sub _uploadopt {
    my ($self, $arg, $value) = @_;
    my $opt;
	$_ = $arg;
    
    if (/consolidate/) {
        $opt = 'ConsolidateIdenticalStructures';
        $value = $value ? 'true' : 'false';
    }
    elsif (/column/) {
        $opt = 'ColumnNamesInFirstRow';
        $value = $value ? 'true' : 'false';
    }
    elsif (/filter/) {
        $opt = 'FilterLikelyNoDataRows';
        $value = $value ? 'true' : 'false';
    }
    elsif (/lock/) {
        $opt = 'LockDataSourceFormat';
        $value = $value ? 'true' : 'false';
    }
    elsif (/ignore/) {
        $opt = 'IgnoreQuotesNotAtStartOrEnd';
        $value = $value ? 'true' : 'false';
    }
    elsif (/skip(_|)at(_|)start/) {
        $opt = 'RowsToSkipAtStart';
    }
    elsif (/skip(_|)at(_|)end/) {
        $opt = 'RowsToSkipAtEnd';
    }
    elsif (/encoding/) {
        $opt = 'CharacterEncoding';
    }

    return ($opt, $value);
}

sub upload {
    my ($self, $filename) = (shift, shift);
    my $space_id = $self->{space_id} || die "No space id set.";
    
    my %opts = @_;
    my @opts = ();
    for (keys %opts) {
        my ($opt, $value) = $self->_uploadopt($_, $opts{$_});
        if ($opt) {
            push @opts, SOAP::Data->name($opt)->value($value);
        }
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
    # we are done so remove upload token
    $self->{upload_token} = 0;
    $som->result || 1;
}

sub upload_sync {
    my $self = shift;
    $self->upload(@_);
    $self->_wait_for_status(\&upload_status);
}

sub process_data {
    my $self = shift;
    my $space_id = $self->{space_id} || die "No space id set.";
    my %opts = @_;
    
    my $date;
    if (exists $opts{date}) {
        $date = parse_datetime($opts{date})->strftime($xsd_datetime_format);
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

*publish_data = \&process_data;

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
    # we are done so clear the publishing token
    $self->{publish_token} = 0;
    $som->result || 1;
}

*publishing_status = \&process_data_status;

sub process_data_sync {
    my $self = shift;
    $self->process_data(@_);
    $self->_wait_for_status(\&process_data_status);
}

*publish_data_sync = \&process_data_sync;

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

sub delete_all_data {
    my $self = shift;
    my $space_id = $self->{space_id} || die "No space id.";
    my $som = $self->_call('deleteAllDataFromSpace',
                 SOAP::Data->name('spaceID')->value($space_id),
            );
    $self->{job_token} = $som->valueof('//deleteAllDataFromSpaceResponse/deleteAllDataFromSpaceResult');
}

sub delete_last_data {
    my $self = shift;
    my $space_id = $self->{space_id} || die "No space id.";
    my $som = $self->_call('deleteLastDataFromSpace',
                 SOAP::Data->name('spaceID')->value($space_id),
            );
    $self->{job_token} = $som->valueof('//deleteLastDataFromSpaceResponse/deleteLastDataFromSpaceResult');
}

sub delete_file_or_dir {
    my ($self, $file) = @_;
    my $space_id = $self->{space_id} || die "No space id.";
    $self->_call('deleteFileOrDirectory',
                 SOAP::Data->name('spaceID')->value($space_id),
                 SOAP::Data->name('fileOrDir')->value($file),
                 );
}

sub job_status {
    my $self = shift;
    my $job_token = $self->{job_token} || die "no job token";
    my $som = $self->_call('isJobComplete',
                 SOAP::Data->name('jobToken')->value($job_token),
            );
    my $result = $som->valueof('//isJobCompleteResponse/isJobCompleteResult');
    if ($result eq 'false') {
        return 0;
    }
    $som = $self->_call('getJobStatus',
                SOAP::Data->name('jobToken')->value($job_token),
            );
    # we are done, so remove job token
    $self->{job_token} = 0;
    $som->result || 1;
}

sub _wait_for_status {
    my ($self, $status_method) = @_;
    if (not ref $status_method eq 'CODE') {
        die "Must supply a status method sub.";
    }
    
    my $result = 0;
    while (not $result) {
        $result = $status_method->($self);
        sleep 10 unless $result;
    }
    $result;
}

sub delete_all_data_sync {
    my $self = shift;
    $self->delete_all_data;
    $self->_wait_for_status(\&job_status);
}

sub delete_last_data_sync {
    my $self = shift;
    $self->data_last_data;
    $self->_wait_for_status(\&job_status);
}

sub sources {
    my $self = shift;
    my $space_id = $self->{space_id} || die "No space id.";
    my $som = $self->_call('getSourcesList',
                 SOAP::Data->name('spaceID')->value($space_id),
            );
    $som->result->{string};    
}

sub source_details {
    my ($self, $source) = @_;
    my $space_id = $self->{space_id} || die "No space id.";
    my $som = $self->_call('getSourceDetails',
                SOAP::Data->name('spaceID')->value($space_id),
                SOAP::Data->name('sourceName')->value($source),
            );
    $som->result;
}

sub _copyopt {
    my ($self, $arg) = @_;
	$_ = $arg;
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
            if ($opts{$_} eq 1) {
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
    join(';', @copy_options);
}

sub _copyspace {
    my ($self, $copy_type, $space_from, $space_to) = (shift, shift, shift, shift);
    my $space_from_id = $self->get_space_id_by_name($space_from);
    my $space_to_id = $self->get_space_id_by_name($space_to);
    my $opts = $self->_buildcopyopts(@_);
    my $som = $self->_call('copySpace',
                 SOAP::Data->name('spFromID')->value($space_from_id),
                 SOAP::Data->name('spToID')->value($space_to_id),
                 SOAP::Data->name('mode')->value($copy_type),
                 SOAP::Data->name('options')->value($opts),
            );
    $self->{job_token} = $som->valueof('//copySpaceResponse/copySpaceResult');
}

sub copy_space {
    my $self = shift;
    $self->_copyspace('copy', @_);
}

sub copy_space_sync {
    my $self = shift;
    $self->copy_space(@_);
    $self->_wait_for_status(\&job_status);
}

sub replicate_space {
    my $self = shift;
    $self->_copyspace('replicate', @_);
}

sub replicate_space_sync {
    my $self = shift;
    $self->replicate_space(@_);
    $self->_wait_for_status(\&job_status);
}

sub delete_space {
    my ($self, $space) = @_;
    my $space_id = get_space_id_by_name($space);
    my $som = $self->_call('deleteSpace',
                 SOAP::Data->name('spaceId')->value($space_id),
            );
    $self->{job_token} = $som->valueof('//deleteSpaceResponse/deleteSpaceResult');
}

sub delete_space_sync {
    my $self = shift;
    $self->delete_space(@_);
    $self->_wait_for_status(\&job_status);
}

sub swap_space_contents {
    my ($self, $space_from, $space_to) = @_;
    my $space_from_id = $self->get_space_id_by_name($space_from);
    my $space_to_id = $self->get_space_id_by_name($space_to);
    my $som = $self->_call('swapSpaceContents',
                           SOAP::Data->name('sp1ID')->value($space_from_id),
                           SOAP::Data->name('sp2ID')->value($space_to_id),
                    );
    $self->{job_token} = $som->valueof('//swapSpaceContentsResponse/swapSpaceContentsResult');
}

sub swap_space_contents_sync {
    my $self = shift;
    $self->swap_space_contents(@_);
    $self->_wait_for_status(\&job_status);
}

sub add_group_acl {
    my ($self, $group, $tag) = @_;
    my $space_id = $self->{space_id} || die "No space id.";
    $self->_call('addAclToGroupInSpace',
                 SOAP::Data->name('groupName')->value($group),
                 SOAP::Data->name('aclTag')->value($tag),
                 SOAP::Data->name('spaceID')->value($space_id),
                 );
}

sub allow_ip_cidr {
    my ($self, $ip) = @_;
    my %opts = @_;
    if (exists $opts{user}) {
        $self->_call('addAllowedIp',
                     SOAP::Data->name('userName')->value($opts{user}),
                     SOAP::Data->name('ip')->value($ip),
                     );
    }
    else {
        $self->_call('addAllowedIPAddrForAccount',
                     SOAP::Data->name('ip')->value($ip),
                     );
    }
}

sub add_group {
    my ($self, $group) = @_;
    my $space_id = $self->{space_id} || die "No space id.";
    $self->_call('addGroupToSpace',
                 SOAP::Data->name('groupName')->value($group),
                 SOAP::Data->name('spaceID')->value($space_id),
                 );
}

sub add_openid {
    my ($self, $user, $openid) = @_;
    $self->_call('addOpenID',
                 SOAP::Data->name('userName')->value($user),
                 SOAP::Data->name('openID')->value($openid),
                 );
}

sub add_proxy_user {
    my ($self, $user, $proxyuser, $expire) = @_;
    my $xsd_expire = parse_datetime($expire)->strftime($xsd_datetime_format);
    $self->_call('addProxyUser',
                 SOAP::Data->name('userName')->value($user),
                 SOAP::Data->name('proxyUserName')->value($proxyuser),
                 SOAP::Data->name('expiration')->value($xsd_expire),
                 );
}

sub add_user {
    my ($self, $user) = (shift, shift);
    my %opts = @_;
    my $params = join(' ', map { $_ . "=" . $opts{$_} } \
                      grep { defined $opts{$_} } qw(email password));
    $self->_call('addUser',
                 SOAP::Data->name('userName')->value($user),
                 SOAP::Data->name('additionalParams')->value($params),
                 );
}

sub add_user_to_group {
    my ($self, $user, $group) = @_;
    my $space_id = $self->{space_id} || die "No space id.";
    $self->_call('addUserToGroupInSpace',
                 SOAP::Data->name('userName')->value($user),
                 SOAP::Data->name('groupName')->value($group),
                 SOAP::Data->name('spaceID')->value($space_id),
                 );
}

sub add_user_to_space {
    my ($self, $user) = (shift, shift);
    my $space_id = $self->{space_id} || die "No space id.";
    my %opts = @_;
    my $admin = $opts{admin} == 1 ? 'true' : 'false';
    $self->_call('addUserToSpace',
                 SOAP::Data->name('userName')->value($user),
                 SOAP::Data->name('spaceID')->value($space_id),
                 SOAP::Data->name('hasAdmin')->value($admin),
                );
}

sub copy_catalog_dir {
    my ($self, $space_from, $space_to, $dir) = @_;
    my $som = $self->_call('copyCatalogDirectory',
                 SOAP::Data->name('spFromID')->value($space_from),
                 SOAP::Data->name('spToID')->value($space_to),
                 SOAP::Data->name('directoryName')->value($dir),
                );
    $som->valueof('//copyCatalogDirectoryResponse/copyCatalogDirectoryResult') eq 'true';
}

sub copy_subject_area {
    my ($self, $space_from, $space_to, $area) = @_;
    $self->_call('copyCustomSubjectArea',
                 SOAP::Data->name('fromSpaceId')->value($space_from),
                 SOAP::Data->name('toSpaceId')->value($space_to),
                 SOAP::Data->name('customSubjectAreaName')->value($area),
                 );
}

sub create_space {
    my ($self, $space) = @_;
    my %opts = @_;
    my $comment = $opts{comment} || '';
    my $automatic = $opts{automatic} == 1 ? 'true' : 'false';
    my @params = (
        SOAP::Data->name('spaceName')->value($space),
        SOAP::Data->name('comments')->value($comment),
        SOAP::Data->name('automatic')->value($automatic),
    );
    my $som;
    if (defined $opts{schema}) {
        push @params, SOAP::Data->name('schemaName')->value($opts{schema});
        $som = $self->_call('createNewSpaceUsingSchema', @params);
    }
    else {
        $som = $self->_call('createNewSpace', @params);
    }
    $som->valueof('//createNewSpaceResponse/createNewSpaceResult') eq 'true';
}

sub delete_user {
    my ($self, $user) = @_;
    $self->_call('deleteUser',
                 SOAP::Data->name('userName')->value($user),
                );
}

sub enable_account {
    my ($self, $account_id) = @_;
    $self->_call('enableAccount',
                 SOAP::Data->name('accountID')->value($account_id),
                 SOAP::Data->name('enable')->value('true'),
                 );
}

sub disable_account {
    my ($self, $account_id) = @_;
    $self->_call('enableAccount',
                 SOAP::Data->name('accountID')->value($account_id),
                 SOAP::Data->name('enable')->value('false'),
                 );
}

sub enable_source {
    my ($self, $source) = @_;
    my $space_id = $self->{space_id} || die "No space id.";
    $self->_call('enableSourceInSpace',
                 SOAP::Data->name('spaceID')->value($space_id),
                 SOAP::Data->name('dataSourceName')->value($source),
                 SOAP::Data->name('enabled')->value('true'),
                 );
}

sub disable_source {
    my ($self, $source) = @_;
    my $space_id = $self->{space_id} || die "No space id.";
    $self->_call('enableSourceInSpace',
                 SOAP::Data->name('spaceID')->value($space_id),
                 SOAP::Data->name('dataSourceName')->value($source),
                 SOAP::Data->name('enabled')->value('false'),
                 );
}

sub enable_user {
    my ($self, $user) = @_;
    $self->_call('enableUser',
                 SOAP::Data->name('userName')->value($user),
                 SOAP::Data->name('enable')->value('true'),
                );
}

sub disable_user {
    my ($self, $user) = @_;
    $self->_call('enableUser',
                 SOAP::Data->name('userName')->value($user),
                 SOAP::Data->name('enable')->value('false'),
                );
}

sub execute_report_schedule {
    my ($self, $report) = @_;
    my $space_id = $self->{space_id} || die "No space id.";
    $self->_call('executeScheduledReport',
                 SOAP::Data->name('spaceId')->value($space_id),
                 SOAP::Data->name('reportScheduleName')->value($report),
                 );
}

my @export_types = qw(CSV PDF PNG PPT RTF XLS);

sub export_report {
    my ($self, $report, $type) = (shift, shift, shift);
    my $space_id = $self->{space_id} || die "No space id.";
    $type = uc $type;
    die "Unknown report type: $type" if none { $type eq $_ } @export_types;
    my %opts = @_;
    my @filters = ();
    if (ref $opts{filters} eq 'ARRAY') {
        for (@{$opts{filters}}) {
            die "Must be a Birst::Filter obeject." unless ref $_ eq 'Birst::Filter';
            push @filters, $_->soapify;
        }
    }
    my @params = (
        SOAP::Data->name('spaceId')->value($space_id),
        SOAP::Data->name('reportPath')->value($report),
    );
    if (@filters) {
        push @params, SOAP::Data->name('reportFilters')->value(\@filters);
    }
    my $som = $self->_call('exportReportTo' . $type, @params);
    $self->{job_token} = $som->valueof('//exportReportTo' . $type . 'Response/exportReportTo' . $type . 'Result');
}

sub export_report_sync {
    my ($self, $file) = (shift, shift);
    $self->export_report(@_);
    $self->{job_token} = $self->{export_token};
    $self->_wait_for_status(\&job_status);
    my $som = $self->_call('getExportData',
                        SOAP::Data->name('exportToken')->value($self->{export_token}),
                        );
    die "No report data available." if not $som->result;
    open(my $fh, ">:raw", $file) or die "Unable to open file: $!";
    print $fh decode_base64($som->result);
    close($fh);
}

sub get_dir_contents {
    my ($self, $dir) = @_;
    my $space_id = $self->{space_id} || die "No space id.";
    my $som = $self->_call('getDirectoryContents',
                 SOAP::Data->name('spaceID')->value($space_id),
                 SOAP::Data->name('dir')->value($dir),
                );
    my $result = $som->result;
    if ($result) {
        if (ref $result->{children} eq 'HASH') {
            $result->{children} = $result->{children}->{FileNode};
        }
    }
    $result;
}

sub get_dir_perms {
    my ($self, $dir) = @_;
    my $space_id = $self->{space_id} || die "No space id.";
    my $som = $self->_call('getDirectoryPermissions',
                           SOAP::Data->name('spaceID')->value($space_id),
                           SOAP::Data->name('dir')->value($dir),
                           );
    my $result = $som->result;
    my $fixed_result;
    if (ref $result eq 'HASH') {
        for(@{$result->{GroupPermission}}) {
            my $group = delete $_->{groupName};
            $fixed_result->{$group} = $_;
        }
    }
    $fixed_result;
}

=encoding utf8

=head1 NAME

Birst::API - Wrapper for the Birst Data Warehouse SOAP API.

=head1 SYNOPSIS

    my $api = Birst::API(version => 5.13);
    $api->login($username, $password);
    $api->set_space_by_name('My Space');
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

=head2 spaces

    $client->spaces;

=head2 get_space_id_by_name

    my $space_id = $client->get_space_id_by_name('My Space Name');

=head2 set_space_id, set_space_by_name

    $client->set_space_id($space_id);
    $client->set_space_by_name('My Space Name');

Sets the space to be used.

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
