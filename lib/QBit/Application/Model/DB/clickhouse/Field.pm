package QBit::Application::Model::DB::clickhouse::Field;

use qbit;

use base qw(QBit::Application::Model::DB::Field);

our %DATA_TYPES = (
    Date        => 'EMPTY',
    UInt8       => 'EMPTY',
    UInt32      => 'EMPTY',
    UInt64      => 'EMPTY',
    Enum8       => 'ENUM',
    Enum16      => 'ENUM',
    FixedString => 'STRING'
);

our %FIELD2STR = (
    EMPTY => sub {
        return $_->quote_identifier($_->name) . ' ' . $_->type;
    },
    ENUM => sub {
        my $self = $_;

        my $value = 0;

        return
            $self->quote_identifier($self->name) . ' '
          . $self->type . '('
          . join(', ', map {$self->quote($_) . ' = ' . ++$value} @{$self->{'values'}}) . ')';
    },
    STRING => sub {
        return $_->quote_identifier($_->name) . ' ' . $_->type . '(' . $_->{'length'} . ')';
    },
);

our %QUOTE_TYPE = (
    Date        => 'STRING',
    UInt8       => 'NUMBER',
    UInt32      => 'NUMBER',
    UInt64      => 'NUMBER',
    Enum8       => 'STRING',
    Enum16      => 'STRING',
    FixedString => 'STRING'
);

sub create_sql {
    my ($self) = @_;

    return $FIELD2STR{$DATA_TYPES{$self->type}}($self);
}

sub init_check {
    my ($self) = @_;

    $self->SUPER::init_check();

    throw gettext('Unknown type: %s', $self->{'type'})
      unless exists($DATA_TYPES{$self->{'type'}});
}

sub quote {
    my ($self, $value) = @_;
    #TODO: rewrite on C++

    if ($QUOTE_TYPE{$self->type} eq 'STRING') {
        $value =~ s/\\/\\\\/g;
        $value =~ s/'/\\'/g;

        return "'$value'";
    }

    return $value;
}

TRUE;
