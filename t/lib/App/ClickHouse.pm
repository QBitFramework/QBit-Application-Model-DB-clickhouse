package App::ClickHouse;

use qbit;

use base qw(QBit::Application::Model::DB::clickhouse);

__PACKAGE__->meta(
    tables => {
        stat => {
            fields => [
                {name => 'dt', type => 'Date'},
                {name => 'f1', type => 'FixedString', length => 512},
                {name => 'f2', type => 'UInt8',},
                {name => 'f3', type => 'Enum8', values => ['a', 'b']},
            ],
            engine => 'MergeTree(dt, (dt, f2), 8192)',
        },
    },
);

TRUE;
