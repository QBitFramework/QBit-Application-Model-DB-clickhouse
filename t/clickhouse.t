#!/usr/bin/perl

use strict;
use warnings;
use utf8;

use Data::Dumper;

use App;

my $app = App->new();

$app->pre_run();

#$app->clickhouse->begin();

my $query = $app->clickhouse->query->select(
    table  => $app->clickhouse->stat,
    alias  => 't1',
    fields => {
        'dt' => '',
        sum  => {SUM => ['f2']},
        undefined => \undef,
    },    #[qw(dt f1 f2 f3)],
    filter => [
        'AND',
        [
            ['dt', 'IN', $app->clickhouse->query->select(alias => 't2', table => $app->clickhouse->stat, fields => [qw(dt)])]
        ]
      ]    #error  f1 => '5'
);

$query->group_by('dt');

$query->limit(2);

$query->calc_rows(1);

print Dumper($query->get_sql_with_data);

print Dumper($query->get_all());

print Dumper($query->found_rows);

#$app->clickhouse->_do($app->clickhouse->create_sql(qw(stat)));
#exit;
#

#print Dumper(
#    $app->clickhouse->stat->add_multi(
#        [
#            {dt => '2017-08-27', f1 => 'hiho', f2 => '34',  f3 => 'a',},
#            {dt => '2017-08-28', f1 => 'man',  f2 => 23,    f3 => 'b',},
#            {dt => '2017-08-29', f1 => '!',    f2 => 15, f3 => 'b',}
#        ]
#    )
#);
#
#my $sql = 'SELECT dt, f1, f2 FROM stat';
#
#my $data = $app->clickhouse->_get_all($sql);
#
#print Dumper($data);

$app->post_run();

print "#END\n";
