package MySQL::Partition::List;
use strict;
use warnings;
use utf8;

use parent 'MySQL::Partition';

sub _build_partition_part {
    my ($self, $partition_name, $value) = @_;

    sprintf 'PARTITION %s VALUES IN (%s)', $partition_name, $value;
}

1;
