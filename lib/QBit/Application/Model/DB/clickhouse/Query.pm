package QBit::Application::Model::DB::clickhouse::Query;

use qbit;

use base qw(QBit::Application::Model::DB::Query);

sub _found_rows {
    my ($self) = @_;

    throw 'Not implemented';
}

#кажется но это не нужно...
sub _field_to_sql {
    my ($self, $alias, $expr, $cur_query_table, %opts) = @_;

    throw Exception::BadArguments gettext('Table field aliase must be SCALAR') if ref($alias);

    $opts{'offset'} ||= 0;

    if (!defined($expr)) {
        return ('NULL');

    } elsif (ref($expr) eq 'SCALAR') {
        # {name => \'string or number'}
        return ($self->quote($$expr) . (defined($alias) ? ' AS ' . $self->quote_identifier($alias) : ''));

    } elsif (!ref($expr) && $expr eq '') {
        # {field_name => ''}
        my $field = $cur_query_table->{'table'}->_fields_hs()->{$alias}
          || throw Exception::BadArguments gettext('Table "%s" has not field "%s"', $cur_query_table->{'table'}->name,
            $alias);

        return (
            map {
                    $self->quote_identifier($self->_table_alias($cur_query_table)) . '.'
                  . $self->quote_identifier($alias . $_) . ' AS '
                  . $self->quote_identifier($alias . ($field->{'i18n'} && $self->{'__ALL_LANGS__'} ? $_ : ''))
              } $field->{'i18n'} ? $self->_get_locale_suffixes() : ('')
        );

    } elsif (!ref($expr)) {
        # {new_field_name => 'field_name'}
        my $field = $cur_query_table->{'table'}->_fields_hs()->{$expr}
          || throw Exception::BadArguments gettext('Table "%s" has not field "%s"', $cur_query_table->{'table'}->name,
            $expr);

        return (
            map {
                    $self->quote_identifier($self->_table_alias($cur_query_table)) . '.'
                  . $self->quote_identifier($expr . $_)
                  . (
                    defined($alias)
                    ? ' AS '
                      . $self->quote_identifier($alias . ($field->{'i18n'} && $self->{'__ALL_LANGS__'} ? $_ : ''))
                    : ''
                  )
              } $field->{'i18n'} ? $self->_get_locale_suffixes() : ('')
        );

    } elsif (
        ref($expr) eq 'HASH'
        && (!ref([%$expr]->[1])
            || (blessed([%$expr]->[1]) && [%$expr]->[1]->isa('QBit::Application::Model::DB::Table')))
      )
    {
        # {alias => {field_name => 'tbl_alias'}} {alias => {field_name => $...db->tbl}}
        my $query_table = $self->_get_table([%$expr]->[1]);
        my $field       = $query_table->{'table'}->_fields_hs()->{[%$expr]->[0]}
          || throw Exception::BadArguments gettext('Table "%s" has not field "%s"', $query_table->{'table'}->name,
            [%$expr]->[0]);

        return (
            map {
                    $self->quote_identifier($self->_table_alias($query_table)) . '.'
                  . $self->quote_identifier([%$expr]->[0] . $_)
                  . (
                    defined($alias)
                    ? ' AS '
                      . $self->quote_identifier($alias . ($field->{'i18n'} && $self->{'__ALL_LANGS__'} ? $_ : ''))
                    : ''
                  )
              } $field->{'i18n'} ? $self->_get_locale_suffixes() : ('')
        );

    } elsif (ref($expr) eq 'HASH' && ref([%$expr]->[1]) eq 'ARRAY') {
        # Function: {field => [SUM => ['f1', \5, ['-' => ['f2', 'f3']]]]}
        my @res       = ();
        my $func_name = [%$expr]->[0];
        $func_name =~ s/^\s+|\s+$//g;
        my @arg_sets =
          map {[$self->_field_to_sql(undef, $_, $cur_query_table, %opts, offset => $opts{'offset'} + 4)]}
          @{[%$expr]->[1]};
        my @locale_suffixes =
          $self->{'__ALL_LANGS__'} && (grep {@$_ > 1} @arg_sets) ? $self->_get_locale_suffixes() : ('');

        for my $i (0 .. @locale_suffixes - 1) {
            my @args = map {@$_ > 1 ? $_->[$i] : $_->[0]} @arg_sets;
            push(@res,
                    "$func_name("
                  . CORE::join(', ', @args) . ')'
                  . (defined($alias) ? ' AS ' . $self->quote_identifier($alias . $locale_suffixes[$i]) : ''));
        }
        return @res;

    } elsif (ref($expr) eq 'ARRAY' && @$expr == 2 && ref($expr->[1]) eq 'ARRAY') {
        # Expression: {field => ['+' => ['f1', 'f2', [f3 => '/' => \5]]]}
        my $offset   = ' ' x $opts{'offset'};
        my @res      = ();
        my $operator = $expr->[0];
        $operator =~ s/^\s+|\s+$//g;
        my @operand_sets =
          map {[$self->_field_to_sql(undef, $_, $cur_query_table, %opts, offset => $opts{'offset'} + 4)]} @{$expr->[1]};

        my @locale_suffixes =
          $self->{'__ALL_LANGS__'} && (grep {@$_ > 1} @operand_sets) ? $self->_get_locale_suffixes() : ('');

        for my $i (0 .. @locale_suffixes - 1) {
            my @operands = map {@$_ > 1 ? $_->[$i] : $_->[0]} @operand_sets;
            if (in_array($operator, [qw(AND OR)])) {
                push(@res,
                        "(\n$offset    "
                      . CORE::join("\n$offset    $operator ", @operands)
                      . "\n$offset)"
                      . (defined($alias) ? ' AS ' . $self->quote_identifier($alias . $locale_suffixes[$i]) : ''));
            } else {
                push(@res,
                        '('
                      . CORE::join(" $operator ", @operands) . ')'
                      . (defined($alias) ? ' AS ' . $self->quote_identifier($alias . $locale_suffixes[$i]) : ''));
            }
        }

        return @res;

    } elsif (ref($expr) eq 'ARRAY' && @$expr == 3) {
        # Comparison: [field => '=' => \5], [field => '=' => \[5, 10, 15]]
        my $offset = ' ' x $opts{'offset'};
        my @res    = ();
        my ($cmp1, $operator, $cmp2) = @$expr;

        if (ref($cmp2) eq 'REF' && ref($$cmp2) eq 'ARRAY' && !@{$$cmp2}) {
            return $offset . 'NULL';
        }

        $expr->[1] =~ s/^\s+|\s+$//g;
        # Fix operator
        $operator = uc($operator);
        $operator =~ s/!=/<>/;
        $operator =~ s/==/=/;
        $operator = 'IS'     if $operator eq '='  && !defined($expr->[2]);
        $operator = 'IS NOT' if $operator eq '<>' && !defined($expr->[2]);
        $operator = 'IN'     if $operator eq '='  && ref($expr->[2]) eq 'REF' && ref(${$expr->[2]}) eq 'ARRAY';
        $operator = 'NOT IN' if $operator eq '<>' && ref($expr->[2]) eq 'REF' && ref(${$expr->[2]}) eq 'ARRAY';

        $cmp1 = [$self->_field_to_sql(undef, $cmp1, $cur_query_table, %opts, offset => $opts{'offset'} + 4)];

        if (ref($cmp2) eq 'REF' && ref($$cmp2) eq 'ARRAY') {
            $cmp2 = ['(' . CORE::join(', ', map {$self->quote($_)} @{$$cmp2}) . ')'];
        } elsif ($operator =~ /ANY|ALL/ && blessed($cmp2) && $cmp2->isa(__PACKAGE__)) {
            ($cmp2) = $cmp2->get_sql_with_data(offset => $opts{'offset'} + 4);
            $cmp2 = ["(\n$offset    $cmp2\n$offset)"];
        } else {
            $cmp2 = [$self->_field_to_sql(undef, $cmp2, $cur_query_table, %opts, offset => $opts{'offset'} + 4)];
        }

        my @locale_suffixes = $self->{'__ALL_LANGS__'} && @$cmp1 + @$cmp2 > 2 ? $self->_get_locale_suffixes() : ('');

        for my $i (0 .. @locale_suffixes - 1) {
            push(@res,
                    (exists($cmp1->[$i]) ? $cmp1->[$i] : $cmp1->[0])
                  . " $operator "
                  . (exists($cmp2->[$i]) ? $cmp2->[$i] : $cmp2->[0]));
        }

        return (
            (@res > 1 ? '(' . CORE::join(' OR ', @res) . ')' : $res[0])
            . (
                defined($alias)
                ? ' AS ' . $self->quote_identifier($alias)
                : ''
              )
        );
    } else {
        throw Exception::BadArguments gettext('Bad field expression:\n%s', Dumper($expr));
    }
}

TRUE;
