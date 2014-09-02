package Birst::Space;
use strict;
use warnings FATAL => 'all';

use Moose;
use Moose::Util::TypeConstraints;
use MooseX::Params::Validate;
use IO::Compress::Zip qw(zip $ZipError);
use MIME::Base64 qw(encode_base64 decode_base64);
use Path::Tiny;
use File::Temp qw(:seekable);
use DateTime;
use Birst::API;

use Data::Dump;


BEGIN {
    require Birst::Filter;
    require DateTime::Format::Flexible;
}

*parse_datetime = \&DateTime::Format::Flexible::parse_datetime;

our $VERSION = '0.01';

subtype 'Birst::SpaceId',
    as 'Str',
    where { /\w{8}-\w{4}-\w{4}-\w{4}-\w{12}/ },
    message { "Space Id '$_' is not a GUID" };

has 'space_id' => (
    is => 'rw',
    isa => 'Birst::SpaceId',
    predicate => 'has_space_id',
);

has 'client' => (
    is => 'ro',
    isa => 'Birst::API',
    writer => '_client',
);

enum 'Birst::ExportType', [qw(CSV PDF PNG PPT RTF XLS)];

has 'upload_token' => (
    is => 'ro',
    isa => 'Str',
    writer => '_upload_token',
    clearer => 'clear_upload_token',
    predicate => 'has_upload_token',
);

has 'publish_token' => (
    is => 'ro',
    isa => 'Str',
    writer => '_publish_token',
    clearer => 'clear_publish_token',
    predicate => 'has_publish_token',
);

has 'export_token' => (
    is => 'ro',
    isa => 'Str',
    writer => '_export_token',
    clearer => 'clear_export_token',
    predicate => 'has_export_token',
);

sub BUILD {
    my ($self, $params) = @_;
    $self->_client($params->{client});
    die "No space id provided." unless (defined $params->{id});
    $self->space_id($params->{id});
}

sub query {
    my $self = shift;
    my ($query) = pos_validated_list(\@_, { isa => 'Str' });
    my $som = $self->client->_call('executeQueryInSpace',
        [
            SOAP::Data->name('query')->value($query),
            SOAP::Data->name('spaceID')->value($self->space_id),
        ],
    );
    require Birst::QueryResult;
    return Birst::QueryResult->new($self->client, $som);
}

sub clear_cache {
    my $self = shift;
    my $som = $self->client->_call('clearCacheInSpace',
        [ SOAP::Data->name('spaceID')->value($self->space_id) ],
    );
    my $result = $som->valueof('//clearCacheInSpaceResponse/clearCacheInSpaceResult');
    return ($result eq 'true');
}

sub clear_dashboard_cache {
    my $self = shift;
    $self->client->_call('clearDashboardCache',
        [ SOAP::Data->name('spaceID')->value($self->space_id) ],
    );
}

sub _uploadopt {
    my $self = shift;
    my ($arg, $value) = pos_validated_list(\@_,
        { isa => 'Str' },
        { isa => 'Str' },
    );

    my $opt;
    local $_ = $arg;
    
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
    my $self = shift;
    my ($filename) = pos_validated_list([shift, shift], { isa => 'Str' });
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
    
    my $som = $self->client->_call('beginDataUpload',
        [
            SOAP::Data->name('spaceID')->value($self->space_id),
            SOAP::Data->name('sourceName')->value($upload_filename),
        ],
    );
    
    my $upload_token = $som->valueof('//beginDataUploadResponse/beginDataUploadResult');
    
    if (@opts) {
        $self->client->_call('setDataUploadOptions',
            [
                SOAP::Data->name('dataUploadToken')->value($upload_token),
                SOAP::Data->name('options')->value(\@opts),
            ],
        );
    }
    
    $fh->seek(0, SEEK_SET);
    my ($data, $bytes);
    my $chunk_size = exists $opts{chunk_size} ? $opts{chunk_size} : 65535;
    while ($bytes = read($fh, $data, $chunk_size)) {
        $data = encode_base64($data);
        $self->client->_call('uploadData',
            [
                SOAP::Data->name('dataUploadToken')->value($upload_token),
                SOAP::Data->name('numBytes')->value($bytes),
                SOAP::Data->name('data')->value($data),
            ],
        );
    }
    
    $self->client->_call('finishDataUpload',
        [ SOAP::Data->name('dataUploadToken')->value($upload_token) ],
    );
    
    $self->_upload_token($upload_token);
}

sub upload_status {
    my $self = shift;
    my $som = $self->client->_call('isDataUploadComplete',
        [ SOAP::Data->name('dataUploadToken')->value($self->upload_token) ],
    );
    my $result = $som->valueof('//isDataUploadCompleteResponse/isDataUploadCompleteResult');
    return 0 if ($result eq 'false');
    $som = $self->client->_call('getDataUploadStatus',
        [ SOAP::Data->name('dataUploadToken')->value($self->upload_token) ],
    );
    $self->clear_upload_token;
    return ($som->result || 1);
}

sub upload_sync {
    my $self = shift;
    $self->upload(@_);
    $self->_wait_for_status(\&upload_status);
}

sub process_data {
    my ($self, %opts) = validated_hash(\@_,
        date      => { isa => 'Str', optional => 1 },
        subgroups => { isa => 'Str | ArrayRef[Str]', optional => 1 },
    );
    
    my $date;
    if (defined $opts{date}) {
        $date = parse_datetime($opts{date})->strftime($Birst::API::xsd_datetime_format);
    }
    else {
        $date = DateTime->now->strftime($Birst::API::xsd_datetime_format);
    }
    
    my @subgroups = ();
    if (defined $opts{subgroups}) {
        if (ref $opts{subgroups} eq 'ARRAY') {
            @subgroups = map { SOAP::Data->name('string')->value($_) } @{$opts{subgroups}};
        }
        else {
            @subgroups = (SOAP::Data->name('string')->value($opts{subgroups}));
        }
    }
    
    my $som = $self->client->_call('publishData',
        [
            SOAP::Data->name('spaceID')->value($self->space_id),
            SOAP::Data->name('subgroups')->value(\@subgroups),
            SOAP::Data->name('date')->value($date),
        ],
    );
    $self->_publish_token($som->valueof('//publishDataResponse/publishDataResult'));
}

*publish_data = \&process_data;

sub process_data_status {
    my $self = shift;
    my $som = $self->client->_call('isPublishingComplete',
        [ SOAP::Data->name('publishingToken')->value($self->publish_token) ],
    );
    my $result = $som->valueof('//isPublishingCompleteResponse/isPublishingCompleteResult');
    return 0 if ($result eq 'false');
    $som = $self->client->_call('getPublishingStatus',
        [ SOAP::Data->name('publishingToken')->value($self->publish_token) ],
    );
    $self->clear_publish_token;
    return ($som->result || 1);
}

*publishing_status = \&process_data_status;

sub process_data_sync {
    my $self = shift;
    $self->process_data(@_);
    $self->_wait_for_status(\&process_data_status);
}

*publish_data_sync = \&process_data_sync;

sub create_subject_area {
    my $self = shift;
    my ($name, $description) = pos_validated_list([shift, shift],
        { isa => 'Str' },
        { isa => 'Str' },
    );
    my %opts = validated_hash(\@_,
        groups => { isa => 'Str | ArrayRef[Str]', optional => 1 }
    );
    
    my @groups = ();
    if (defined $opts{groups}) {
        if (ref $opts{groups} eq 'ARRAY') {
            @groups = map { SOAP::Data->name('string')->value($_) } @{$opts{groups}};
        }
        else {
            @groups = (SOAP::Data->name('string')->value($opts{groups}));
        }
    }
    
    $self->client->_call('createSubjectArea',
        [
            SOAP::Data->name('spaceID')->value($self->space_id),
            SOAP::Data->name('name')->value($name),
            SOAP::Data->name('description')->value($description),
            SOAP::Data->name('groups')->value(\@groups),
        ],
    );
}

sub delete_subject_area {
    my $self = shift;
    my ($name) = pos_validated_list(\@_, { isa => 'Str' });
    $self->client->_call('deleteSubjectArea',
        [
            SOAP::Data->name('spaceID')->value($self->space_id),
            SOAP::Data->name('name')->value($name),
        ],
    );
}

sub create_directory {
    my $self = shift;
    my ($dir) = pos_validated_list(\@_, { isa => 'Str' });
    my $directory = path($dir)->basename;
    my $parent = path($dir)->dirname;
    $parent =~ s/\/$//;
    
    my $som = $self->client->_call('checkAndCreateDirectory',
        [
            SOAP::Data->name('spaceID')->value($self->space_id),
            SOAP::Data->name('parentDir')->value($parent),
            SOAP::Data->name('newDirectoryName')->value($directory),
        ],
    );
    my $result = $som->valueof('//checkAndCreateDirectoryResponse/checkAndCreateDirectoryResult');
    return ($result eq 'true');
}

sub get_engine_version {
    my $self = shift;
    my $som = $self->client->_call('getSpaceProcessEngineVersion',
        [ SOAP::Data->name('spaceID')->value($self->space_id) ],
    );
    $som->valueof('//getSpaceProcessEngineVersionResponse/getSpaceProcessEngineVersionResult');
}

sub set_engine_version {
    my $self = shift;
    my ($version) = pos_validated_list(\@_, { isa => 'Birst::Version' });
    my $som = $self->client->_call('setSpaceProcessEngineVersion',
        [
            SOAP::Data->name('spaceID')->value($self->space_id),
            SOAP::Data->name('processingVersionName')->value($version),
        ],
    );
}

sub delete_all_data {
    my $self = shift;
    my $som = $self->client->_call('deleteAllDataFromSpace',
        [ SOAP::Data->name('spaceID')->value($self->space_id) ],
    );
    $self->client->_job_token($som->valueof('//deleteAllDataFromSpaceResponse/deleteAllDataFromSpaceResult'));
}

sub delete_last_data {
    my $self = shift;
    my $som = $self->client->_call('deleteLastDataFromSpace',
        [ SOAP::Data->name('spaceID')->value($self->space_id) ],
    );
    $self->client->_job_token($som->valueof('//deleteLastDataFromSpaceResponse/deleteLastDataFromSpaceResult'));
}

sub delete_file_or_dir {
    my $self = shift;
    my ($file) = pos_validated_list(\@_, { isa => 'Str' });
    $self->client->_call('deleteFileOrDirectory',
        [
            SOAP::Data->name('spaceID')->value($self->space_id),
            SOAP::Data->name('fileOrDir')->value($file),
        ],
    );
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
    my $som = $self->client->_call('getSourcesList',
        [ SOAP::Data->name('spaceID')->value($self->space_id) ],
    );
    return $som->result->{string};    
}

sub source_details {
    my $self = shift;
    my ($source) = pos_validated_list(\@_, { isa => 'Str' });
    my $som = $self->client->_call('getSourceDetails',
        [
            SOAP::Data->name('spaceID')->value($self->space_id),
            SOAP::Data->name('sourceName')->value($source),
        ],
    );
    return $som->result;
}

sub add_group_acl {
    my $self = shift;
    my ($group, $acl) = pos_validated_list(\@_,
        { isa => 'Str' },
        { isa => 'Str' },
    );
    $self->client->_call('addAclToGroupInSpace',
        [
            SOAP::Data->name('groupName')->value($group),
            SOAP::Data->name('aclTag')->value($acl),
            SOAP::Data->name('spaceID')->value($self->space_id),
        ],
    );
}

sub add_group {
    my $self = shift;
    my ($group) = pos_validated_list(\@_, { isa => 'Str' });
    $self->client->_call('addGroupToSpace',
        [
            SOAP::Data->name('groupName')->value($group),
            SOAP::Data->name('spaceID')->value($self->space_id),
        ],
    );
}

sub add_user_to_group {
    my $self = shift;
    my ($user, $group) = pos_validated_list(\@_,
        { isa => 'Str' },
        { isa => 'Str' },
    );
    $self->client->_call('addUserToGroupInSpace',
        [
            SOAP::Data->name('userName')->value($user),
            SOAP::Data->name('groupName')->value($group),
            SOAP::Data->name('spaceID')->value($self->space_id),
        ],
    );
}

sub add_user {
    my $self = shift;
    my ($user) = pos_validated_list([shift], { isa => 'Str' });
    my ($is_admin) = validated_list(\@_,
        admin => { isa => 'Bool', default => 0 }
    );
    my $admin = $is_admin ? 'true' : 'false';
    $self->client->_call('addUserToSpace',
        [
            SOAP::Data->name('userName')->value($user),
            SOAP::Data->name('spaceID')->value($self->space_id),
            SOAP::Data->name('hasAdmin')->value($admin),
        ],
    );
}

sub enable_source {
    my $self = shift;
    my ($source) = pos_validated_list(\@_, { isa => 'Str' });
    $self->client->_call('enableSourceInSpace',
        [
            SOAP::Data->name('spaceID')->value($self->space_id),
            SOAP::Data->name('dataSourceName')->value($source),
            SOAP::Data->name('enabled')->value('true'),
        ],
    );
}

sub disable_source {
    my $self = shift;
    my ($source) = pos_validated_list(\@_, { isa => 'Str' });
    $self->client->_call('enableSourceInSpace',
        [
            SOAP::Data->name('spaceID')->value($self->space_id),
            SOAP::Data->name('dataSourceName')->value($source),
            SOAP::Data->name('enabled')->value('false'),
        ],
    );
}

sub run_scheduled_report {
    my $self = shift;
    my ($report) = pos_validated_list(\@_, { isa => 'Str' });
    $self->client->_call('executeScheduledReport',
        [
            SOAP::Data->name('spaceId')->value($self->space_id),
            SOAP::Data->name('reportScheduleName')->value($report),
        ],
    );
}

sub export {
    my $self = shift;
    my ($report, $type) = pos_validated_list([shift, shift],
        { isa => 'Str' },
        { isa => 'Birst::ExportType' },
    );

    my ($filters) = validated_list(\@_,
        filters => { isa => 'ArrayRef[Birst::Filter]', optional => 1 },
    );

    my @filters = ();
    @filters = map { $_->soapify } @$filters if $filters;

    my @params = (
        SOAP::Data->name('spaceId')->value($self->space_id),
        SOAP::Data->name('reportPath')->value($report),
    );
    if (@filters) {
        push @params, SOAP::Data->name('reportFilters')->value(\@filters);
    }
    my $som = $self->client->_call('exportReportTo' . $type, \@params);
    $self->client->_job_token($som->valueof('//exportReportTo' . $type . 'Response/exportReportTo' . $type . 'Result'));
}

sub export_sync {
    my $self   = shift;
    my ($file) = pos_validated_list([shift], { isa => 'Str' });
    $self->export_report(@_);
    $self->client->_job_token($self->export_token);
    $self->_wait_for_status(\&job_status);
    my $som = $self->client->_call('getExportData',
        [ SOAP::Data->name('exportToken')->value($self->export_token) ],
    );
    die "No report data available" if not $som->result;
    open(my $fh, ">:raw", $file) or die "Unable to open file: $!";
    print $fh decode_base64($som->result);
    close($fh);
}

sub dir_contents {
    my $self  = shift;
    my ($dir) = pos_validated_list(\@_, { isa => 'Str' });
    my $som   = $self->client->_call('getDirectoryContents',
        [
            SOAP::Data->name('spaceID')->value($self->space_id),
            SOAP::Data->name('dir')->value($dir),
        ],
    );
    my $result = $som->result;
    if ($result) {
        if (ref $result->{children} eq 'HASH') {
            $result->{children} = $result->{children}->{FileNode};
        }
    }
    return $result;
}

sub dir_perms {
    my $self  = shift;
    my ($dir) = pos_validated_list(\@_, { isa => 'Str' });
    my $som   = $self->client->_call('getDirectoryPermissions',
        [
            SOAP::Data->name('spaceID')->value($self->space_id),
            SOAP::Data->name('dir')->value($dir),
        ],
    );
    my $result = $som->result;
    my $fixed_result;
    if (ref $result eq 'HASH') {
        for(@{$result->{GroupPermission}}) {
            my $group = delete $_->{groupName};
            $fixed_result->{$group} = $_;
        }
    }
    return $fixed_result;
}

1;
