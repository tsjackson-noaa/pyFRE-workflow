"""Transliteration of FRE/lib/FRENamelists.pm.
"""
import re

import logging
_log = logging.getLogger(__name__)

# Global constants -------------------------------------------------------------

PATTERN_BOOLEAN  = r'(\.true\.|\.false\.|t|f)'
PATTERN_INTEGER  = r'([+-]?\d+)'
PATTERN_SQSTRING = r"'([^']*)'";
PATTERN_DQSTRING = r'"([^"]*)"';
PATTERN_DATE     = r'(\d+\s*,\s*\d+\s*,\s*\d+\s*,\s*\d+\s*,\s*\d+\s*,\s*\d+)'
PATTERN_LAYOUT   = r'((?:\d+|\$\w+)\s*,\s*(?:\d+|\$\w+))'
PATTERN_TYPELESS = r/'\S.*)'

PATTERN_TAIL_BREAK = r'(\b)'
PATTERN_TAIL_ALL = r'((?:[,\n]\s*\w+(?:\s*\[\s*\d+(?:\s*,\s*\d+)*\s*\])?\s*=\s*.*)*)'


# Utilities --------------------------------------------------------------------

def namelistValueGet(object, namelistName, variable, pattern, tail):
    content = getattr(object, namelistName)
    content = re.sub(r'^\s*!.*$v.*$', r'', content)
    if re.match('\b$v\s*=\s*$p$t', re.IGNORECASE):
        return $1 # XXX
    else:
        return None


def namelistValuePut(object, namelistName, variable, pattern, tail):

    # ------ arguments: $object $namelistName $variable $value $pattern $tail
{
    my ( $r, $n, $v, $x, $p, $t ) = @_;
    my ( $content, $mark, @commentedLines ) = ( $r->{$n}, 'FRENamelists::PLACEHOLDER', () );
    while ( $content =~ m/^(\s*!.*$v.*)$/m ) {
        substr( $content, $-[0], $+[0] - $-[0] ) = $mark;
        push @commentedLines, $1;
    }
    if ( $content =~ m/\b$v\s*=\s*$p$t/i ) {
        substr( $content, $-[0], $+[0] - $-[0] ) = "$v = $x$2";
    }
    else {
        $content = "\t$v = $x\n" . $content;
    }
    foreach my $commentedLine (@commentedLines) {
        substr( $content, $-[0], $+[0] - $-[0] ) = $commentedLine if $content =~ m/$mark/;
    }
    $r->{$n} = $content;
};

# //////////////////////////////////////////////////////////////////////////////
# ////////////////////////////////// Class initialization/copying/termination //
# //////////////////////////////////////////////////////////////////////////////

sub new($)

    # ------ arguments: $className
    # ------ create empty namelist set
{
    my ( $class, $r ) = ( shift, {} );
    bless $r, $class;
    return $r;
}

sub DESTROY

    # ------ arguments: $object
    # ------ called automatically
{
}

# //////////////////////////////////////////////////////////////////////////////
# ////////////////////////////////////// Exported Functions - Namelists Level //
# //////////////////////////////////////////////////////////////////////////////

sub copy($)

    # ------ arguments: $object
    # ------ return a deep copy of the object
{
    my ( $r, $s ) = ( shift, FRENamelists->new() );
    foreach my $name ( keys %{$r} ) { $s->{$name} = $r->{$name} }
    return $s;
}

sub names($)

    # ------ arguments: $object
    # ------ return a list of namelist names
{
    my $r = shift;
    return sort keys %{$r};
}

sub asFortranString($)

    # ------ arguments: $object
    # ------ called as object method
{
    my ( $r, $s ) = ( shift, '' );
    foreach my $name ( sort keys %{$r} ) { $s .= $r->namelistAsFortranString($name) }
    return $s;
}

sub asXMLString($)

    # ------ arguments: $object
    # ------ called as object method
{
    my ( $r, $s ) = ( shift, '' );
    foreach my $name ( sort keys %{$r} ) { $s .= $r->namelistAsXMLString($name) }
    return $s;
}

# //////////////////////////////////////////////////////////////////////////////
# ////////////////////////////// Exported Functions - A Single Namelist Level //
# //////////////////////////////////////////////////////////////////////////////

sub namelistExists($$)

    # ------ arguments: $object $namelistName
    # ------ called as object method
{
    my ( $r, $n ) = @_;
    return exists( $r->{$n} );
}

sub namelistGet($$)

    # ------ arguments: $object $namelistName
    # ------ called as object method
{
    my ( $r, $n ) = @_;
    return $r->{$n};
}

sub namelistPut($$$)

    # ------ arguments: $object $namelistName $namelistContent
    # ------ called as object method
{
    my ( $r, $n, $c ) = @_;
    $r->{$n} = $c;
}

sub namelistAsFortranString($$)

    # ------ arguments: $object $namelistName
    # ------ called as object method
{
    my ( $r, $n, $s ) = ( @_, '' );
    $s .= ' &' . $n . "\n";
    $s .= $r->{$n} . "\n";
    $s .= '/' . "\n\n";
    return $s;
}

sub namelistAsXMLString($$)

    # ------ arguments: $object $namelistName
    # ------ called as object method
{
    my ( $r, $n, $s ) = ( @_, '' );
    $s .= '<namelist name="' . $n . '">' . "\n";
    $s .= $r->{$n} . "\n";
    $s .= '</namelist>' . "\n";
    return $s;
}

# //////////////////////////////////////////////////////////////////////////////
# ///////////////////// Exported Functions - A Single Namelist Variable Level //
# //////////////////////////////////////////////////////////////////////////////

sub namelistBooleanGet($$$)

    # ------ arguments: $object $namelistName $variable
{
    my $value = $namelistValueGet->( @_, FRENamelists::PATTERN_BOOLEAN, '' );
    if ( defined($value) ) {
        return ( $value =~ m/t/i ) ? 1 : 0;
    }
    else {
        return undef;
    }
}

sub namelistBooleanPut($$$$)

    # ------ arguments: $object $namelistName $variable $value
{
    $namelistValuePut->(
        @_[ 0, 1, 2 ],
        ( $_[3] ) ? '.true.' : '.false.',
        FRENamelists::PATTERN_BOOLEAN, ''
    );
}

sub namelistIntegerGet($$$)

    # ------ arguments: $object $namelistName $variable
{
    return $namelistValueGet->( @_, FRENamelists::PATTERN_INTEGER,
        FRENamelists::PATTERN_TAIL_BREAK );
}

sub namelistIntegerPut($$$$)

    # ------ arguments: $object $namelistName $variable $value
{
    $namelistValuePut->( @_, FRENamelists::PATTERN_INTEGER, FRENamelists::PATTERN_TAIL_BREAK );
}

sub namelistDoubleQuotedStringGet($$$)

    # ------ arguments: $object $namelistName $variable
{
    return $namelistValueGet->( @_, FRENamelists::PATTERN_DQSTRING, '' );
}

sub namelistDoubleQuotedStringPut($$$$)

    # ------ arguments: $object $namelistName $variable $value
{
    $namelistValuePut->( @_[ 0, 1, 2 ], "\"$_[3]\"", FRENamelists::PATTERN_DQSTRING, '' );
}

sub namelistSingleQuotedStringGet($$$)

    # ------ arguments: $object $namelistName $variable
{
    return $namelistValueGet->( @_, FRENamelists::PATTERN_SQSTRING, '' );
}

sub namelistSingleQuotedStringPut($$$$)

    # ------ arguments: $object $namelistName $variable $value
{
    $namelistValuePut->( @_[ 0, 1, 2 ], "'$_[3]'", FRENamelists::PATTERN_SQSTRING, '' );
}

sub namelistDateGet($$$)

    # ------ arguments: $object $namelistName $variable
{
    return $namelistValueGet->( @_, FRENamelists::PATTERN_DATE, FRENamelists::PATTERN_TAIL_BREAK );
}

sub namelistDatePut($$$$)

    # ------ arguments: $object $namelistName $variable $value
{
    $namelistValuePut->( @_, FRENamelists::PATTERN_DATE, FRENamelists::PATTERN_TAIL_BREAK );
}

sub namelistLayoutGet($$$)

    # ------ arguments: $object $namelistName $variable
{
    return $namelistValueGet->( @_, FRENamelists::PATTERN_LAYOUT,
        FRENamelists::PATTERN_TAIL_BREAK );
}

sub namelistLayoutPut($$$$)

    # ------ arguments: $object $namelistName $variable $value
{
    $namelistValuePut->( @_, FRENamelists::PATTERN_LAYOUT, FRENamelists::PATTERN_TAIL_BREAK );
}

sub namelistTypelessGet($$$)

    # ------ arguments: $object $namelistName $variable
{
    return $namelistValueGet->( @_, FRENamelists::PATTERN_TYPELESS,
        FRENamelists::PATTERN_TAIL_ALL );
}

sub namelistTypelessPut($$$$)

    # ------ arguments: $object $namelistName $variable $value
{
    $namelistValuePut->( @_, FRENamelists::PATTERN_TYPELESS, FRENamelists::PATTERN_TAIL_ALL );
}

# //////////////////////////////////////////////////////////////////////////////
# ///////////////////// Exported Functions - Additional utilities             //
# //////////////////////////////////////////////////////////////////////////////

# Combines base and override namelist content for a single namelist,
#   overwriting and combining as expected.
# Note: This doesn't work properly, as it doesn't respect normal namelist features
#   like multiple definitions per line, embedded newlines, and others.
#   See t/03.override_namelist.t
sub mergeNamelistContent($$)

    # ------ arguments: $base_namelist_content $override_namelist_content
{
    my ( $base_namelist_content, $override_namelist_content ) = @_;

    # store the base namelist as a FRENamelist
    my $nmls = FRENamelists->new();
    $nmls->namelistPut( 'nml', $base_namelist_content );

    # "parse" override namelist into key/value pairs
    # Note: this doesn't respect many namelist features (embedded newlines etc)
    my %override_namelist;
    for ( grep !/^\s*!/, split "\n", $override_namelist_content ) {
        $override_namelist{$1} = $2 if /\s*(\S+)\s*=\s*(\S.*)$/;
    }

    # combine namelists
    $nmls->namelistTypelessPut( 'nml', $_, $override_namelist{$_} )
        for reverse sort keys %override_namelist;

    # return as string
    return $nmls->{'nml'};
} ## end sub mergeNamelistContent($$)

# //////////////////////////////////////////////////////////////////////////////
# //////////////////////////////////////////////////////////// Initialization //
# //////////////////////////////////////////////////////////////////////////////

return 1;
