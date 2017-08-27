package QBit::Application::Model::DB::clickhouse::st;

use qbit;

use base qw(QBit::Class);

__PACKAGE__->mk_ro_accessors(qw(request lwp sql dbi));

__PACKAGE__->mk_accessors(qw(result errstr));

sub execute {
    my ($self, @params) = @_;

    my $sql = $self->sql;

    unless (defined($sql)) {
        $self->dbi->err('CH1');
        $self->errstr(gettext('Statement not found'));

        return undef;
    }

    if (@params) {
        my $db = $self->dbi->db;

        my $i = 0;
        while ($sql =~ s/\?/$db->quote($params[$i])/) {
            $i++;
        }

        if ($i != @params) {
            $self->dbi->err('CH1');
            $self->errstr(gettext('Placeholders(?): %d, parameters: %d', $i, scalar(@params)));

            return undef;
        }
    }

    my $request = $self->request;
    $request->content($sql);

    my $response = $self->lwp->request($request);

    my $content = $response->decoded_content;

    unless ($response->is_success) {
        $self->errstr($content);

        if ($response->code == 500 && $content =~ /^Code:\s+(\d+)/) {
            $self->dbi->err($1);
        } elsif ($response->code == 500) {
            $self->dbi->err('CH2');
        } else {
            $self->dbi->err($response->code);
        }

        return undef;
    }

    my $result;
    try {
        my $res = from_json($content || '{}');

        print "$content\n";

        $self->result($res);

        $result = $res->{'data'} // TRUE;

        $self->dbi->{'__FOUND_ROWS__'} = $res->{'rows_before_limit_at_least'} // 0;

        #TODO: total
    }
    catch {
        $self->dbi->err('CH3');
        $self->errstr(shift->message);
    };

    return $result;
}

sub fetchall_arrayref {
    my ($self, $attr) = @_;

    return $self->result->{'data'} // TRUE;
}

#STH interface

sub finish { }

TRUE;
