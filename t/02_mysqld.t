use strict;
use warnings;
use utf8;
use Test::More;
use Test::mysqld;

my $mysqld = Test::mysqld->new(
    my_cnf => {
      'skip-networking' => '',
    }
) or plan skip_all => $Test::mysqld::errstr;

my @connect_info = ($mysqld->dsn(dbname => 'test'));
$connect_info[3] = {
    RaiseError          => 1,
    PrintError          => 0,
    ShowErrorStatement  => 1,
    AutoInactiveDestroy => 1,
    mysql_enable_utf8   => 1,
};
my $dbh = DBI->connect(@connect_info);

$dbh->do(q[CREATE TABLE `test` (
  `id` BIGINT unsigned NOT NULL auto_increment,
  `event_id` INTEGER NOT NULL,
  PRIMARY KEY (`id`, `event_id`)
)]);

$dbh->do(q[CREATE TABLE `test2` (
  `id` BIGINT unsigned NOT NULL auto_increment,
  `created_at` datetime NOT NULL,
  PRIMARY KEY (`id`, `created_at`)
)]);

$dbh->do(q[CREATE TABLE `test3` (
  `id` BIGINT unsigned NOT NULL auto_increment,
  `created_at` datetime NOT NULL,
  PRIMARY KEY (`id`, `created_at`)
)]);

use MySQL::Partition;

subtest list => sub {
    my $list_partition = MySQL::Partition->new(
        dbh        => $dbh,
        type       => 'list',
        table      => 'test',
        definition => 'event_id',
    );
    isa_ok $list_partition, 'MySQL::Partition::List';

    ok !$list_partition->is_partitioned;
    $list_partition->create_partitions('p1' => 1);
    pass 'create_partitions ok';
    ok $list_partition->is_partitioned;
    ok $list_partition->has_partition('p1');
    my @partitions = $list_partition->retrieve_partitions;
    is_deeply \@partitions, ['p1'];

    subtest 'add_partitions' => sub {
        $list_partition->add_partitions('p2' => '2, 3');
        pass 'add_partitions ok';
        ok $list_partition->has_partition('p2');
        my @partitions = $list_partition->retrieve_partitions;
        is_deeply \@partitions, ['p1', 'p2'];
    };

    subtest 'drop_partition' => sub {
        $list_partition->drop_partition('p1');
        pass 'drop_partition ok';
        ok !$list_partition->has_partition('p1');
        my @partitions = $list_partition->retrieve_partitions;
        is_deeply \@partitions, ['p2'];
    };
};

done_testing;
