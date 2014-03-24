package MySQL::Partition;
use 5.008001;
use strict;
use warnings;

our $VERSION = "0.01";

use MySQL::Partition::Handle;

use Module::Load ();
use Class::Accessor::Lite::Lazy (
    rw      => [qw/dry_run verbose/],
    ro      => [qw/type dbh table definition/],
    ro_lazy => {
        dbname => sub {
            _get_dbname(shift->dbh->{Name});
        },
    },
);

sub new {
    my $class = shift;
    die q[can't call new method directory in sub class] if $class ne __PACKAGE__;
    my %args = @_ == 1 ? %{$_[0]} : @_;

    $args{type} = uc $args{type};

    my ($type) = split /\s+/, $args{type};
    my $sub_class = __PACKAGE__ . '::Type::' . ucfirst( lc $type );
    Module::Load::load($sub_class);
    bless \%args, $sub_class;
}

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
        ORDER BY
          partition_name
    ');
    $sth->execute($self->table, $self->dbname);
    while (my $row = $sth->fetchrow_arrayref) {
        push @parts, $row->[0] if defined $row->[0];
    }
    @parts;
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
    $sth->execute($self->table, $self->dbname, $self->type);
    $sth->rows > 0;
}

sub _build_create_partitions_sql {
    my ($self, @args) = @_;

    if ($self->type eq 'RANGE' && $self->catch_all_partition_name) {
        push @args, $self->catch_all_partition_name, 'MAXVALUE';
    }
    sprintf 'ALTER TABLE %s PARTITION BY %s (%s) (%s)',
        $self->table, $self->type, $self->definition, $self->_build_partition_parts(@args);
}

sub _build_add_partitions_sql {
    my ($self, @args) = @_;

    sprintf 'ALTER TABLE %s ADD PARTITION (%s)', $self->table, $self->_build_partition_parts(@args);
}

sub _build_partition_parts {
    my ($self, @args) = @_;

    my @parts;
    while (my ($partition_name, $value) = splice @args, 0, 2) {
        push @parts, $self->_build_partition_part($partition_name, $value);
    }
    join ', ', @parts;
}

sub _build_partition_part {
    die 'this is abstruct method';
}

sub _build_drop_partition_sql {
    my ($self, $partition_name) = @_;

    sprintf 'ALTER TABLE %s DROP PARTITION %s', $self->table, $partition_name;
}

sub _grow_methods {
    my ($class, @methods) = @_;

    for my $method (@methods) {
        my $prepare_method = "prepare_$method";
        my $sql_builder_method   = "_build_${method}_sql";

        no strict 'refs';
        *{$class . '::' . $prepare_method} = sub {
            use strict 'refs';
            my ($self, @args) = @_;
            my $sql = $self->$sql_builder_method(@args);

            return MySQL::Partition::Handle->new(
                statement       => $sql,
                mysql_partition => $self,
            );
        };
        *{$class . '::' . $method} = sub {
            use strict 'refs';
            my ($self, @args) = @_;
            $self->$prepare_method(@args)->execute;
        };
    }
}
__PACKAGE__->_grow_methods(qw/create_partitions add_partitions drop_partition/);


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

