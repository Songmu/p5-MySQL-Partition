requires 'Class::Accessor::Lite';
requires 'Class::Accessor::Lite::Lazy';
requires 'Module::Load';
requires 'parent';
requires 'perl', '5.010001';

on configure => sub {
    requires 'CPAN::Meta';
    requires 'CPAN::Meta::Prereqs';
    requires 'Module::Build';
};

on test => sub {
    requires 'Test::More', '0.98';
    requires 'Test::mysqld';
};
