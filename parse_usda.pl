#!/usr/bin/env perl

use v5.10.0;
use strict;
use warnings;

use Parse::USDA::NNDB;
use YAML::XS qw/Dump DumpFile/;

my $pusda = Parse::USDA::NNDB->new;
my @tables = $pusda->tables;

#say Dump(\@tables);

#foreach my $table (@tables) {
#    my $data = $pusda->parse_file($table);
#    DumpFile($table. '.yaml', $data);
#}

$pusda->show_food_by_ndb(43404);
