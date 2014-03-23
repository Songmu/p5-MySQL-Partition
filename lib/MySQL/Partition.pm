package MySQL::Partition;
use 5.010001;
use strict;
use warnings;

our $VERSION = "0.01";

use Class::Accessor::Lite::Lazy (
    new     => 1,
    ro      => [qw/type dbh table definition catch_all_partition_name/],
    ro_lazy => {
        dbname => sub {
            _get_dbname(shift->dbh->{Name});
        },
    },
);

sub _get_dbname {
    my $connected_db = shift;

    # XXX can't parse 'host=hoge;database=fuga'
    my ($dbname) = $connected_db =~ m!^(?:(?:database|dbname)=)?([^;]*)!i;
    $dbname;
}

sub retrieve_partitions {
    my ($self, $table) = @_;

    my @parts;
    my $sth = $self->dbh->prepare('
        SELECT
          partition_name
        FROM
          information_schema.PARTITIONS
        WHERE
          table_name   = ? AND
          table_schema = ?
    ');
    $sth->execute($self->table, $self->dbname);
    while (my $row = $sth->fetchrow_arrayref) {
        push @parts, $row->[0] if defined $row->[0];
    }
    return \@parts;
}

sub has_partition {
    my ($self, $partition_name) = @_;

    my $sth = $self->dbh->prepare('
        SELECT
          partition_name,
          partition_ordinal_position
        FROM
          information_schema.PARTITIONS
        WHERE
          table_name     = ? AND
          table_schema   = ? AND
          partition_name = ?
    ');
    $sth->execute($self->table, $self->dbname, $partition_name);
    $sth->rows > 0;
}

sub is_partitioned {
    my $self = shift;
    my $sth = $self->dbh->prepare('
        SELECT partition_name
        FROM
          information_schema.partitions
        WHERE
          table_name       = ? and
          table_schema     = ? and
          partition_method = ?
    ');
    $sth->execute($self->table, $self->get_dbname, $self->type);
    $sth->rows > 0;
}

sub create_partitions {
    my ($self, @args) = @_;

    if ($self->type eq 'RANGE' && $self->catch_all_partition_name) {
        push @args, $self->catch_all_partition_name, 'MAXVALUE';
    }
    sprintf 'ALTER TABLE %s PARTITION BY %s (%s) (%s)',
        $self->table, $self->type, $self->definition, $self->_build_partition_parts(@args);
}

sub add_partitions {
    my ($self, @args) = @_;

    sprintf 'ALTER TABLE %s ADD PARTITION (%s)', $self->table, $self->_build_partition_parts(@args);
}

sub reorganize_catch_all_partition {
    my ($self, @args) = @_;

    sprintf 'ALTER TABLE %s REORGANIZE PARTITION %s INTO (
        %s,
        PARTITION %s VALUES LESS THAN (MAXVALUE)
    )', $self->table, $self->catch_all_partition_name, $self->_build_partition_parts(@args), $self->catch_all_partition_name;
}

sub _build_partition_parts {
    my ($self, @args) = @_;

    my @parts;
    while (my ($partition_name, $value) = splice @args, 0, 2) {
        push @parts, $self->_build_partition_parts($partition_name, $value);
    }
    join ', ', @parts;
}

sub _build_partition_part {
    my ($self, $partition_name, $value) = shift;

    my $type = uc $self->type;
    if ( $type eq 'LIST' ) {
        sprintf 'PARTITION %s VALUES LESS THAN (%s)', $partition_name, $value;
    }
    elsif ($type eq 'RANGE') {
        sprintf 'PARTITION %s VALUES IN (%s)', $partition_name, $value;
    }
}

sub drop_partition {
    my ($self, $partition_name) = @_;

    sprintf 'ALTER TABLE %s DROP PARTITION %s', $self->table, $partition_name;
}

1;
__END__

=encoding utf-8

=head1 NAME

MySQL::Partition - It's new $module

=head1 SYNOPSIS

    use MySQL::Partition;

=head1 DESCRIPTION

MySQL::Partition is ...

=head1 LICENSE

Copyright (C) Songmu.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Songmu E<lt>y.songmu@gmail.comE<gt>

=cut

