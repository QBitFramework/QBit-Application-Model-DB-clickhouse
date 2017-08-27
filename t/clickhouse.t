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
        sum  => {SUM => ['f2']}
    },    #[qw(dt f1 f2 f3)],
    filter => [
        'AND',
        [
            [dt => '=' => \'2017-09-02'],
            #['dt', 'IN', $app->clickhouse->query->select(alias => 't2', table => $app->clickhouse->stat, fields => [qw(dt)])]
        ]
      ]    #error  f1 => '5'
);

$query->group_by('dt');

print Dumper($query->get_sql_with_data);

print Dumper($query->get_all());

exit;

#$app->clickhouse->_do($app->clickhouse->create_sql(qw(stat)));
#exit;

my $sql = 'SELECT dt, f1, f2 FROM stat';

my $data = $app->clickhouse->_get_all($sql);

print Dumper($data);

$app->post_run();

print "#END\n";
