package MySQL::Partition::Type::Range;
use strict;
use warnings;

use parent 'MySQL::Partition';
use Class::Accessor::Lite (
    ro => [qw/catch_all_partition_name/],
);

use MySQL::Partition::Handle;

sub build_add_catch_all_partition_sql {
    my $self = shift;
    die "catch_all_partition_name isn't specified" unless $self->catch_all_partition_name;

    sprintf 'ALTER TABLE %s ADD PARTITION (%s)',
        $self->table, $self->_build_partition_part($self->catch_all_partition_name, 'MAXVALUE');
}

sub build_reorganize_catch_all_partition_sql {
    my ($self, @args) = @_;
    die "catch_all_partition_name isn't specified" unless $self->catch_all_partition_name;

    sprintf 'ALTER TABLE %s REORGANIZE PARTITION %s INTO (%s, PARTITION %s VALUES LESS THAN (MAXVALUE))',
        $self->table, $self->catch_all_partition_name, $self->_build_partition_parts(@args), $self->catch_all_partition_name;
}

sub _build_partition_part {
    my ($self, $partition_name, $value) = @_;

    if ($value !~ /^[0-9]+$/ && $value ne 'MAXVALUE' && $value !~ /\(/) {
        $value = "'$value'";
    }
    sprintf 'PARTITION %s VALUES LESS THAN (%s)', $partition_name, $value;
}

for my $method (qw/add_catch_all_partition reorganize_catch_all_partition/) {
    my $prepare_method = "prepare_$method";
    my $sql_builder_method   = "build_${method}_sql";

    no strict 'refs';
    *{__PACKAGE__ . '::' . $prepare_method} = sub {
        use strict 'refs';
        my ($self, @args) = @_;
        my $sql = $self->$sql_builder_method(@args);

        return MySQL::Partition::Handle->new(
            statement       => $sql,
            mysql_partition => $self,
        );
    };

    *{__PACKAGE__ . '::' . $method} = sub {
        use strict 'refs';
        my ($self, @args) = @_;
        $self->$prepare_method(@args)->execute;
    };
}

1;
