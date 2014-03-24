package MySQL::Partition::Type::Range;
use strict;
use warnings;

use parent 'MySQL::Partition';
use Class::Accessor::Lite (
    ro => [qw/catch_all_partition_name/],
);

__PACKAGE__->_grow_methods(qw/add_catch_all_partition reorganize_catch_all_partition/);

sub _build_add_catch_all_partition_sql {
    my $self = shift;
    die "catch_all_partition_name isn't specified" unless $self->catch_all_partition_name;

    sprintf 'ALTER TABLE %s ADD PARTITION (%s)',
        $self->table, $self->_build_partition_part($self->catch_all_partition_name, 'MAXVALUE');
}

sub _build_reorganize_catch_all_partition_sql {
    my ($self, @args) = @_;
    die "catch_all_partition_name isn't specified" unless $self->catch_all_partition_name;

    sprintf 'ALTER TABLE %s REORGANIZE PARTITION %s INTO (%s, PARTITION %s VALUES LESS THAN (MAXVALUE))',
        $self->table, $self->catch_all_partition_name, $self->_build_partition_parts(@args), $self->catch_all_partition_name;
}

sub _build_partition_part {
    my ($self, $partition_name, $partition_description) = @_;

    if ($partition_description !~ /^[0-9]+$/ && $partition_description ne 'MAXVALUE' && $partition_description !~ /\(/) {
        $partition_description = "'$partition_description'";
    }
    sprintf 'PARTITION %s VALUES LESS THAN (%s)', $partition_name, $partition_description;
}

1;
