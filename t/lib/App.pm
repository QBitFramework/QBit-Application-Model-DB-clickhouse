package App;

use qbit;

use base qw(QBit::Application);

use App::ClickHouse accessor => 'clickhouse';

__PACKAGE__->use_config('App.cfg');

TRUE;
