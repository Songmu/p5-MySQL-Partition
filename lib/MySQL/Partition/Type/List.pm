package MySQL::Partition::Type::List;
use strict;
use warnings;

use parent 'MySQL::Partition';

sub _build_partition_part {
    my ($self, $partition_name, $partition_description) = @_;

    my $comment;
    if (ref $partition_description && ref $partition_description eq 'HASH') {
        $comment = $partition_description->{comment};
        $comment =~ s/'//g if defined $comment;
        $partition_description = $partition_description->{description};
        die 'no partition_description is specified' unless $partition_description;
    }
    my $part = sprintf 'PARTITION %s VALUES IN (%s)', $partition_name, $partition_description;
    $part .= " COMMENT = '$comment'" if $comment;
    $part;
}

1;
