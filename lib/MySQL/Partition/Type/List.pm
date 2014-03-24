package MySQL::Partition::Type::List;
use strict;
use warnings;

use parent 'MySQL::Partition';

sub _build_partition_part {
    my ($self, $partition_name, $value) = @_;

    sprintf 'PARTITION %s VALUES IN (%s)', $partition_name, $value;
}

1;
