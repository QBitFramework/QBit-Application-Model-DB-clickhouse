package QBit::Application::Model::DB::clickhouse::db;

use qbit;

use base qw(QBit::Class);

use LWP::UserAgent;
use HTTP::Request;

use QBit::Application::Model::DB::clickhouse::st;

__PACKAGE__->mk_ro_accessors(qw(db));

sub init {
    my ($self) = @_;

    $self->{'__REQUEST__'} =
      HTTP::Request->new(POST => sprintf('http://%s:%s/?database=%s', @$self{qw(host port database)}));

    $self->{'__LWP__'} = LWP::UserAgent->new(timeout => $self->{'timeout'});
}

sub prepare {
    my ($self, $sql) = @_;

    return QBit::Application::Model::DB::clickhouse::st->new(
        request => $self->{'__REQUEST__'},
        lwp     => $self->{'__LWP__'},
        sql     => $sql,
        db      => $self->db
    );
}

#DBI interface

sub do {
    my ($self, $sql, $attr, @params) = @_;

    my $sth = $self->prepare($sql);

    return $sth->execute(@params);
}

sub err {''}

sub errstr {''}

TRUE;
