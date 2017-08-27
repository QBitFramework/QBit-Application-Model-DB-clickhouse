package QBit::Application::Model::DB::clickhouse::Field;

use qbit;

use base qw(QBit::Application::Model::DB::Field);

our %DATA_TYPES = (
    Date        => {field_type => 'EMPTY',  quote_type => 'STRING',},
    UInt8       => {field_type => 'EMPTY',  quote_type => 'NUMBER',},
    UInt32      => {field_type => 'EMPTY',  quote_type => 'NUMBER',},
    UInt64      => {field_type => 'EMPTY',  quote_type => 'NUMBER',},
    Enum8       => {field_type => 'ENUM',   quote_type => 'STRING',},
    Enum16      => {field_type => 'ENUM',   quote_type => 'STRING',},
    FixedString => {field_type => 'STRING', quote_type => 'STRING',},
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

sub create_sql {
    my ($self) = @_;

    return $FIELD2STR{$DATA_TYPES{$self->type}->{'field_type'}}($self);
}

sub init_check {
    my ($self) = @_;

    $self->SUPER::init_check();

    throw gettext('Unknown type: %s', $self->{'type'})
      unless exists($DATA_TYPES{$self->{'type'}});
}

sub quote {
    my ($self, $value) = @_;
    #TODO: rewrite(C++)

    return 'NULL' unless defined($value);

    if ($DATA_TYPES{$self->type}->{'quote_type'} eq 'STRING') {
        $value =~ s/\\/\\\\/g;
        $value =~ s/'/\\'/g;

        return "'$value'";
    }

    return $value;
}

TRUE;
