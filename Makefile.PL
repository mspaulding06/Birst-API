use 5.006;
use strict;
use warnings FATAL => 'all';
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME             => 'Birst::API',
    AUTHOR           => q{Matt Spaulding <mspaulding06@gmail.com>},
    VERSION_FROM     => 'lib/Birst/API.pm',
    ABSTRACT_FROM    => 'lib/Birst/API.pm',
    LICENSE          => 'Artistic_2_0',
    PL_FILES         => {},
    MIN_PERL_VERSION => 5.006,
    CONFIGURE_REQUIRES => {
        'ExtUtils::MakeMaker' => 0,
    },
    BUILD_REQUIRES => {
        'Test::More' => 0,
    },
    PREREQ_PM => {
        'DateTime' => 0,
        'DateTime::Format::Flexible' => 0,
        'SOAP::Lite' => 0,
        'HTTP::Cookies' => 0,
        'IO::Compress::Zip' => 0,
        'File::Temp' => 0,
        'MIME::Base64' => 0,
        'Path::Tiny' => 0,
    },
    dist  => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean => { FILES => 'Birst-API-*' },
);
