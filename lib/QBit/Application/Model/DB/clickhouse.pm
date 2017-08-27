package QBit::Application::Model::DB::clickhouse;

use qbit;

use base qw(QBit::Application::Model::DB);

use QBit::Application::Model::DB::clickhouse::dbi;
use QBit::Application::Model::DB::clickhouse::Table;
use QBit::Application::Model::DB::clickhouse::Query;
use QBit::Application::Model::DB::Filter;

use Exception::DB;
eval {require Exception::DB::DuplicateEntry};

__PACKAGE__->model_accessors(clickhouse => 'QBit::Application::Model::API::Yandex::ClickHouse',);

BEGIN {
    no strict 'refs';

    foreach my $method (qw(begin commit rollback transaction)) {
        *{__PACKAGE__ . "::$method"} = sub {throw gettext('Method "%s" not supported', $method)}
    }
}

my $REQUEST;

sub query {
    my ($self) = @_;

    return QBit::Application::Model::DB::clickhouse::Query->new(db => $self);
}

sub filter {
    my ($self, $filter, %opts) = @_;

    return QBit::Application::Model::DB::Filter->new($filter, %opts, db => $self);
}

sub _create_sql_db {
    my ($self) = @_;

    return 'CREATE DATABASE ' . $self->get_dbh()->quote_identifier($self->get_option('database'));
}

sub _get_table_class {
    my ($self, %opts) = @_;

    my $table_class;
    if (defined($opts{'type'})) {
        my $try_class = "QBit::Application::Model::DB::clickhouse::Table::$opts{'type'}";
        $table_class = $try_class if eval("require $try_class");

        throw gettext('Unknown table class "%s"', $opts{'type'}) unless defined($table_class);
    } else {
        $table_class = 'QBit::Application::Model::DB::clickhouse::Table';
    }

    return $table_class;
}

sub _connect {
    my ($self, %opts) = @_;

    unless (defined($self->dbh())) {
        foreach (qw(host port database user password)) {
            $opts{$_} //= $self->get_option($_, '');
        }

        $self->set_dbh(
            QBit::Application::Model::DB::clickhouse::dbi->new(
                %opts,
                timeout => $self->get_option('timeout', 300),
                db      => $self
            )
        );
    }
}

sub _is_connection_error {
    my ($self, $code) = @_;

    return !!grep {$code eq $_} qw(CH2);
}

sub quote_identifier {"`$_[1]`"}

sub quote {
    my ($self, $name) = @_;
    #TODO: rewrite(C++)

    return 'NULL' unless defined($name);

    unless (looks_like_number($name)) {
        my $quote = $name;
        $quote =~ s/\\/\\\\/g;
        $quote =~ s/'/\\'/g;

        return "'$quote'";
    }

    return $name;
}

sub _get_all {
    my ($self, $sql, @params) = @_;

    $sql .= ' FORMAT JSON';

    return $self->SUPER::_get_all($sql, @params);
}

TRUE;
