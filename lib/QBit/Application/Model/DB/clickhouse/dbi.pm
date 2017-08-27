package QBit::Application::Model::DB::clickhouse::dbi;

use qbit;

use base qw(QBit::Class);

use LWP::UserAgent;
use HTTP::Request;

use QBit::Application::Model::DB::clickhouse::st;

__PACKAGE__->mk_ro_accessors(qw(db));

__PACKAGE__->mk_accessors(qw(err errstr));

sub init {
    my ($self) = @_;

    $self->{'__REQUEST__'} =
      HTTP::Request->new(
        POST => sprintf('http://%s:%s/?database=%s&user=%s&password=%s', @$self{qw(host port database user password)}));

    $self->{'__LWP__'} = LWP::UserAgent->new(timeout => $self->{'timeout'});
}

sub prepare {
    my ($self, $sql) = @_;

    return QBit::Application::Model::DB::clickhouse::st->new(
        request => $self->{'__REQUEST__'},
        lwp     => $self->{'__LWP__'},
        sql     => $sql,
        dbi     => $self
    );
}

sub do {
    my ($self, $sql, $attr, @params) = @_;

    my $sth = $self->prepare($sql);

    my $res = $sth->execute(@params);

    $self->errstr($sth->errstr()) unless $res;

    return $res;
}

TRUE;
