package MySQL::Partition::Type::List;
use strict;
use warnings;

use parent 'MySQL::Partition';

sub _build_partition_part {
    my ($self, $partition_name, $partition_description) = @_;

    sprintf 'PARTITION %s VALUES IN (%s)', $partition_name, $partition_description;
}

1;
