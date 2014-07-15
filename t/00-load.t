#!perl -T
use 5.006;
use strict;
use warnings FATAL => 'all';
use Test::More;

plan tests => 1;

BEGIN {
    use_ok( 'Birst::API' ) || print "Bail out!\n";
}

diag( "Testing Birst::API $Birst::API::VERSION, Perl $], $^X" );
