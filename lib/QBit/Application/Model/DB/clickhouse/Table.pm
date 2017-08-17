package QBit::Application::Model::DB::clickhouse::Table;

use qbit;

use base qw(QBit::Application::Model::DB::Table);

use QBit::Application::Model::DB::clickhouse::Field;

our $ADD_CHUNK = 1000;

sub add {
    my ($self, $data, %opts) = @_;

    $self->add_multi([$data], %opts);

    return undef;
}

sub add_multi {
    my ($self, $data, %opts) = @_;

    my $fields = $self->_fields_hs();

    my $data_fields;
    if ($opts{'identical_rows'}) {
        $data_fields = [keys(%{$data->[0] // {}})];
    } else {
        $data_fields = array_uniq(map {keys(%$_)} @$data);
    }

    my $field_names;
    if ($opts{'ignore_extra_fields'}) {
        $field_names = arrays_intersection([map {$fields->{$_}->name} keys %$fields], $data_fields);
    } else {
        my @unknown_fields = grep {!exists($fields->{$_})} @$data_fields;

        throw gettext('In table %s not found follows fields: %s', $self->name(), join(', ', @unknown_fields))
          if @unknown_fields;

        $field_names = $data_fields;
    }

    throw Exception::BadArguments gettext('Expected fields') unless $field_names;

    my @locales = keys(%{$self->db->get_option('locales', {})});
    @locales = (undef) unless @locales;

    my $add_rows = 0;

    my $sql_header = 'INSERT INTO ' . $self->quote_identifier($self->name) . ' (';

    my @real_field_names;
    foreach my $name (@$field_names) {
        if ($fields->{$name}{'i18n'}) {
            push(@real_field_names, defined($_) ? "${name}_${_}" : $name) foreach @locales;
        } else {
            push(@real_field_names, $name);
        }
    }
    $sql_header .= join(', ', map {$self->quote_identifier($_)} @real_field_names) . ") VALUES\n";

    my $db = $self->db();

    while (my @add_data = splice(@$data, 0, $ADD_CHUNK)) {
        my ($delimiter, $values) = ('', '');

        foreach my $row (@add_data) {
            my @params;
            foreach my $name (@$field_names) {
                if ($fields->{$name}{'i18n'}) {
                    if (ref($row->{$name}) eq 'HASH') {
                        my @missed_langs = grep {!exists($row->{$name}{$_})} @locales;
                        throw Exception::BadArguments gettext('Undefined languages "%s" for field "%s"',
                            join(', ', @missed_langs), $name)
                          if @missed_langs;
                        push(@params, $fields->{$name}->quote($row->{$name}{$_})) foreach @locales;
                    } elsif (!ref($row->{$name})) {
                        push(@params, $fields->{$name}->quote($row->{$name})) foreach @locales;
                    } else {
                        throw Exception::BadArguments gettext('Invalid value in table->add');
                    }
                } else {
                    push(@params, $fields->{$name}->quote($row->{$name}));
                }
            }

            $values .= "$delimiter(" . join(', ', @params) . ')';

            $delimiter ||= ",\n";
        }

        $db->_do($sql_header . $values);

        $add_rows += @add_data;
    }

    return $add_rows;
}

sub create_sql {
    my ($self) = @_;

    throw gettext('Inherites does not realize') if $self->inherits;

    my $engine = defined($self->engine()) ? $self->engine() : 'MergeTree';

    return
        'CREATE TABLE '
      . $self->quote_identifier($self->name)
      . " (\n    "
      . join(",\n    ", (map {$_->create_sql()} @{$self->fields}),) . "\n"
      . ") ENGINE = $engine;\n";
}

sub _get_field_object {
    my ($self, %opts) = @_;

    return QBit::Application::Model::DB::clickhouse::Field->new(%opts);
}

TRUE;
