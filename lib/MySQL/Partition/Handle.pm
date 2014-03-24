package MySQL::Partition::Handle;
use strict;
use warnings;

use Class::Accessor::Lite (
    new => 1,
    ro  => [qw/mysql_partition statement/],
    rw  => [qw/_executed/],
);

sub execute {
    my $self = shift;
    die 'statement is already executed' if $self->_executed;

    my $mysql_partition = $self->mysql_partition;
    my $sql             = $self->statement;
    if ($mysql_partition->verbose || $mysql_partition->dry_run) {
        printf "Following SQL statement to be executed%s.\n", ($mysql_partition->dry_run ? ' (dry-run)' : '');
        print "$sql\n";
    }
    if (!$mysql_partition->dry_run) {
        $mysql_partition->dbh->do($sql);
        print "done.\n" if $mysql_partition->verbose;
    }
    $self->_executed(1);
}

1;
