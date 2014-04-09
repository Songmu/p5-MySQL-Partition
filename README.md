[![Build Status](https://travis-ci.org/Songmu/p5-MySQL-Partition.png?branch=master)](https://travis-ci.org/Songmu/p5-MySQL-Partition) [![Coverage Status](https://coveralls.io/repos/Songmu/p5-MySQL-Partition/badge.png?branch=master)](https://coveralls.io/r/Songmu/p5-MySQL-Partition?branch=master)
# NAME

MySQL::Partition - Utility for MySQL partitioning

# SYNOPSIS

    use MySQL::Partition;
    my $dbh = DBI->connect(@connect_info);
    my $list_partition = MySQL::Partition->new(
        dbh        => $dbh,
        type       => 'list',
        table      => 'test',
        expression => 'event_id',
    );
    $list_partition->is_partitioned;
    $list_partition->create_partitions('p1' => 1); # ALTER TABLE test PARTITION BY LIST ...
    $list_partition->has_partition('p1'); # true
    

    $list_partition->add_partitions('p2_3' => '2, 3');
    

    # handle interface
    my $handle = $list_partition->prepare_add_partitions('p4' => 4);
    print $handle->statement;
    $handle->execute;
    

    $list_partition->drop_partitions('p1');
    $handle = $list_partition->prepare_drop_partitions('p2_3');
    $handle->execute;

# DESCRIPTION

MySQL::Partition is utility module for MySQL partitions.

This module creates a object for manipulating MySQL partitions.
This is very useful that we no longer write complicated and MySQL specific SQL syntax any more.

__THE SOFTWARE IS STILL ALPHA QUALITY. API MAY CHANGE WITHOUT NOTICE.__

# INTERFACE

## Constructor

### `my $mysql_partition:MySQL::Partition = MySQL::Partition->new(%args)`

Create a new object which is subclass of [MySQL::Partition](http://search.cpan.org/perldoc?MySQL::Partition).
([MySQL::Partition::Type::Range](http://search.cpan.org/perldoc?MySQL::Partition::Type::Range) or [MySQL::Partition::Type::List](http://search.cpan.org/perldoc?MySQL::Partition::Type::List).

[%args](http://search.cpan.org/perldoc?%args) must contain following keys.

- `dbh => DBI::st`
- `table => Str`
- `type => Str`

    partitioning method. `list(?: columns)?` or `range(?: columns)?`.

    If `list` is specified, `new` method returns `MySQL::Partition::Type::List` object.

- `expression => Str`

    partitioning expression. e.g. `event_id`, `created_at`, `TO_DAYS(created_at)`, etc.

## Methods

### `my @partition_names = $mysql_partition->retrieve_partitions`

Returns partition names in the table.

### `my $bool = $mysql_partition->is_partitioned`

Returns the table is partitioned or not.

### `my $bool = $mysql_partition->has_partitione($partition_name)`

Returns the table has a specified partition name or not.

## Methods for manipulating partition

### `$mysql_partition->create_partitions($partition_name => $partition_description, [$name => $description, ...])`

### `$mysql_partition->add_partitions($partition_name => $partition_description, [$name => $description], ...)`

### `$mysql_partition->drop_partitions(@partition_names)`

## Methods for MySQL::Partition::Handle

Each method for manipulating partition has `prepare_*` method which returns [MySQL::Partition::Handle](http://search.cpan.org/perldoc?MySQL::Partition::Handle) object.

- `prepare_create_partitions`
- `prepare_add_partitions`
- `prepare_drop_partitions`

Actually, `$mysql_partition->create_partitions(...);` is a shortcut of following.

    my $handle = $mysql_partition->prepare_create_partitions(...);
    $handle->execute;

# LICENSE

Copyright (C) Songmu.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

# AUTHOR

Songmu <y.songmu@gmail.com>
