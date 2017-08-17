package QBit::Application::Model::DB::clickhouse::st;

use qbit;

use base qw(QBit::Class);

__PACKAGE__->mk_ro_accessors(qw(request lwp sql db));

__PACKAGE__->mk_accessors(qw(result));

sub execute {
    my ($self, @params) = @_;

    #TODO: check modifie, may be need clone
    my $sql = $self->sql;

    if (@params) {
        $sql =~ s/\?/$self->db->quote($_)/e foreach @params;
    }

    my $request = $self->request;
    $request->content($sql);

    my ($response, $res);
    try {
        $response = $self->lwp->request($request);

        unless ($response->is_success) {
            my $error = $response->decoded_content;

            if ($response->code == 500 && $error =~ /^Code:\s+(\d+)/) {
                throw Exception::DB $error, errorcode => $1;
            } elsif ($response->code == 500) {
                throw Exception::DB $error, errorcode => -1;
            } else {
                throw Exception::DB $error, errorcode => $response->code;
            }
        }

        $res = from_json($response->decoded_content || '{}')->{'data'};
    }
    catch {
        my ($exception) = shift;

        throw $exception if $exception->isa('Exception::DB');

        throw Exception::DB $exception->message, errorcode => -2;
    };

    return $self->result($res);
}

sub fetchall_arrayref {
    my ($self, $attr) = @_;

    return $self->result();
}

#STH interface

sub finish {}

TRUE;
