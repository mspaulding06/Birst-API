#!/usr/bin/perl

use 5.012;
use warnings;
use utf8;

use Birst::API;
use Data::Dumper;

my $client = Birst::API->new(version => 5.13);

$client->login('matt.spaulding@fresno.edu', 'Sunbirds1');
$client->set_space_by_name('Matt - Budget Final');
$client->query('SELECT [Sum: Credit], [Sum: Debit] FROM [ALL]');
my $result = $client->fetch;
print Dumper $result;

