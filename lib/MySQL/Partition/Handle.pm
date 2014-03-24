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

    $self->mysql_partition->_execute($self->statement);
    $self->_executed(1);
}

1;
