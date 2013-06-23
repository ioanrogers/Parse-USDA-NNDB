package Parse::USDA::NNDB;
{
  $Parse::USDA::NNDB::VERSION = '0.2';
}

# ABSTRACT: download and parse the latest USDA National Nutrient Database

use v5.10.1;
use Moo;

use autodie;
use Archive::Zip qw/ :ERROR_CODES :CONSTANTS /;
use Carp qw/croak/;
use File::Fetch;
use File::HomeDir;

#use IO::Uncompress::Unzip;
use Log::Any;
use methods;
use Text::CSV_XS;
use Path::Tiny;
use URI;

no if $] >= 5.018, 'warnings', 'experimental::smartmatch';

with qw/MooX::Log::Any/;

# XXX file encoding
# TODO use the updates rather than a whole new db
# http://www.ars.usda.gov/SP2UserFiles/Place/12354500/Data/SR25/dnload/sr25upd.zip
# XXX option to download old releases?
# TODO progress bars...


has base_dir => ( is => 'lazy' );
has _data_dir => ( is => 'lazy' );
has _src_uri  => ( is => 'lazy' );
has _src_file => ( is => 'lazy',);
has _fh => ( is => 'rw', predicate => 1, clearer => 1);
has _csv => (is => 'lazy', predicate => 1, clearer => 1);


has tables => (
    is      => 'ro',
    default => sub {
        [
            qw/DATSRCLN FD_GROUP LANGUAL LANGDESC FOOTNOTE NUTR_DEF SRC_CD DATA_SRC DERIV_CD FOOD_DES NUT_DATA WEIGHT/
        ];
    },
);

method _build_base_dir () {

    # TODO better cross-platform defaults
    my $dir = path(File::HomeDir->my_home, '.cache/usda_nndb');
    if (!$dir->exists) {
        $dir->mkpath;
    }
    return $dir;
}

method _build__data_dir () {
    my $dir = $self->base_dir->child('sr24');
    if (!$dir->exists) {
        $dir->mkpath;
    }
    return $dir;
}

method _build__src_uri () {
    URI->new(
        'http://www.ars.usda.gov/SP2UserFiles/Place/12354500/Data/SR24/dnload/sr24.ZIP'
    );
    #http://www.ars.usda.gov/SP2UserFiles/Place/12354500/Data/SR25/dnload/sr25.zip
}

method _build__src_file () {
    $self->base_dir->child('sr24.ZIP');
}

method _build__csv () {
    return Text::CSV_XS->new({
        quote_char     => '~',
        escape_char    => '~',
        sep_char       => '^',
        empty_is_undef => 1,
        binary         => 1,
    });
}

method _log_croak ($msg) {
    $self->log->crit($msg);
    croak $msg;
}

method _log_croakf (@args) {
    my $msg = sprintf shift @args, @args;
    $self->_log_croak($msg);
}

method _get_file_path_for ($table) {
    my $file = $self->_data_dir->child("$table.txt");
    $self->logger->debug("Using path [$file] for '$table'");
    if (!$file->exists) {
        $self->_extract_data;
    }

    return $file;
}


method get_columns_for ($table) {

    given ($table) {
        when (/^FOOD_DES$/i) {
            return [
                qw/NDB_No FdGrp_Cd Long_Desc Shrt_Desc ComName ManufacName Survey Ref_desc Refuse SciName N_Factor Pro_Factor Fat_Factor CHO_Factor/
            ];
        }
        when (/^FD_GROUP$/i) {
            return [qw/FdGrp_Cd FdGrp_Desc/];
        }
        when (/^LANGUAL$/i) {
            return [qw/NDB_No Factor_Code/];
        }
        when (/^LANGDESC$/i) {
            return [qw/Factor_Code Description/];
        }
        when (/^NUT_DATA$/i) {
            return [
                qw/NDB_No Nutr_No Nutr_Val Num_Data_Pts Std_Error Src_Cd Deriv_Cd Ref_NDB_No Add_Nutr_Mark Num_Studies Min Max DF Low_EB Up_EB Stat_cmt CC/
            ];

        }
        when (/^NUTR_DEF$/i) {
            return [qw(Nutr_No Units Tagname NutrDesc Num_Dec SR_Order)];
        }
        when (/^SRC_CD$/i) {
            return [qw(Src_Cd SrcCd_Desc)];
        }
        when (/^DERIV_CD$/i) {
            return [qw(Deriv_Cd Deriv_Desc)];
        }
        when (/^WEIGHT$/i) {
            return [
                qw(NDB_No Seq Amount Msre_Desc Gm_Wgt Num_Data_Pts Std_Dev)];
        }
        when (/^FOOTNOTE$/i) {
            return [qw(NDB_No Footnt_No Footnt_Typ Nutr_No Footnt_Txt)];
        }
        when (/^DATSRCLN$/i) {
            return [qw(NDB_No Nutr_No DataSrc_ID)];
        }
        when (/^DATA_SRC$/i) {
            return [
                qw(DataSrc_ID Authors Title Year Journal Vol_City Issue_State Start_Page End_Page)
            ];
        }

        #when ( /^ABBREV$/i ) {
        #    return [
        #        qw(NDB_No Shrt_Desc Water Energ_Kcal Protein Lipit_Tot Ash Carbonhydrt Fiber_TD Sugar_Tot Calcium Iron Magnesium Phosphorus Potassium Sodium Zinc Copper Manganese Selenium Vit_C Thiamin Riboflavin Niacin Panto_acid Vit_B6 Folate_Tot Folic_acid Food_Folate Folate_DFE Choline_total Vit_B12 Vit_A_IU Vit_A_RAE Retinol Alpha_Carot Beta_Carot Beta_Crypt Lycopene Lut_and_Zea Vit_E Vit_K FA_Sat FA_Mono FA_Poly Cholestrl GmWt_1 GmWt_Desc1 GmWt_2 GmWt_Desc2 Refuse_Pct)
        #      ];
        #}
        default {
            return;
        }
    }
}


method table ($table) {
    $table = uc $table;
    my $file_path = $self->_get_file_path_for($table);

    open my $fh, '<:encoding(iso-8859-1)', $file_path
      or $self->_log_croak("Could not open [$file_path]: $!");

    my $column_names = $self->get_columns_for($table)
      or $self->_log_croak("Couldn't find columns for [$table]");

    $self->_csv->column_names($column_names);
    $self->_fh($fh);

    return 1;
}


method get_line () {
    if (!$self->_has_fh) {
        $self->_log_croak(
            'No active filehandle. Did you call \'table\' first?');

        # TODO should check the handle is the *right* handle
    }
    if (!$self->_has_csv) {
        $self->_log_croak('No csv object. Did you call \'table\' first?');
    }

    my $row = $self->_csv->getline_hr($self->_fh);

    if ($self->_csv->eof) {
        $self->logger->debug('Closing file');
        $self->_fh->close;
        $self->_clear_fh;
        $self->_clear_csv;
    }

    #    use Data::Printer; p $row;
    my ($code, $str, $pos) = $self->_csv->error_diag;
    if ($str && !$self->_csv->eof) {

        $self->_log_croakf('CSV parse error at pos %s: %s [%s]',
            $pos, $str, $self->_csv->error_input);
    }

    return $row;
}

method _fetch_data () {

    # does zip already exist?
    # TODO any checksums we can use?

    my $ff = File::Fetch->new(uri => $self->_src_uri);

    $self->logger->debug(
        "Downloading " . $self->_src_uri . " to " . $self->base_dir);
    my $file = $ff->fetch(to => $self->base_dir)
      or $self->logger->log_and_croak($ff->error);

    $self->logger->debug("Saved data to $file");

    return 1;
}

method _extract_data () {
    if (!$self->_src_file->exists) {
        $self->_fetch_data;
    }

    $self->log->debug('Extracting file ' . $self->_src_file);
    my $zip = Archive::Zip->new;

    unless ($zip->read($self->_src_file->stringify) == AZ_OK) {
        $self->logger->error('Read error');
        croak;
    }

    my $ok = $zip->extractTree(undef, $self->_data_dir->stringify . '/');

    return 1 if $ok == AZ_OK;
    croak "Failed to extract file";
}

1;

__END__

=pod

=encoding utf-8

=for :stopwords Ioan Rogers

=head1 NAME

Parse::USDA::NNDB - download and parse the latest USDA National Nutrient Database

=head1 VERSION

version 0.2

=head1 SYNOPSIS

  use Parse::USDA::NNDB;
  my $usda = Parse::USDA::NNDB->new;
  $usda->table( 'fd_group' );
  while (my $fg = $usda->getline) {
      printf "ID: %s  DESC: %s\n", $fg->{NDB_No}, $fg->{Shrt_Desc};
  }

=head1 DESCRIPTION

Parse::USDA::NNDB is for parsing the nutrient data files made available by the
USDA in ASCII format. If the files are not available, they will be automatically
retrieved and extracted for you.

=head1 EXTENDS

=over 4

=item * L<Moo::Object>

=back

=head1 ATTRIBUTES

=head2 C<base_dir>

This is ithe directory into which the data files will be downloaded.
Defaults to $HOME/.cache/usda_nndb

=head2 C<tables>

An arrayref of all the known tables.

=head2 C<get_columns_for($table)>

Returns an arrayref of the column names used in this table, or undef if the
table is unknown

=head1 METHODS

=head2 C<new($basedir)>

Creates a new Parse::USDA::NNDB object. Takes one optional argument, a path
to the dir which will store the datafiles to be parsed.

=head2 C<table ($table)>

Given a case-insenstive table name, this method sets the current active table
and opens the file ready for parsing. You must call this before B<get_line>.

Returns true on success, throws an exception on error.

=head2 C<get_line>

Returns the next line in the data file and returns a hashref
(see USDA docs for their meanings).

Returns undef when the file is finished or if something goes wrong.

=head1 SEE ALSO

L<USDA documentation|http://www.ars.usda.gov/Services/docs.htm?docid=8964>

=head1 BUGS AND LIMITATIONS

You can make new bug reports, and view existing ones, through the
web interface at L<https://github.com/ioanrogers/Parse-USDA-NNDB/issues>.

=head1 AVAILABILITY

The project homepage is L<http://metacpan.org/release/Parse-USDA-NNDB/>.

The latest version of this module is available from the Comprehensive Perl
Archive Network (CPAN). Visit L<http://www.perl.com/CPAN/> to find a CPAN
site near you, or see L<https://metacpan.org/module/Parse::USDA::NNDB/>.

=head1 SOURCE

The development version is on github at L<http://github.com/ioanrogers/Parse-USDA-NNDB>
and may be cloned from L<git://github.com/ioanrogers/Parse-USDA-NNDB.git>

=head1 AUTHOR

Ioan Rogers <ioanr@cpan.org>

=head1 COPYRIGHT AND LICENSE

This software is Copyright (c) 2013 by Ioan Rogers.

This is free software, licensed under:

  The Artistic License 2.0 (GPL Compatible)

=head1 DISCLAIMER OF WARRANTY

BECAUSE THIS SOFTWARE IS LICENSED FREE OF CHARGE, THERE IS NO WARRANTY
FOR THE SOFTWARE, TO THE EXTENT PERMITTED BY APPLICABLE LAW. EXCEPT
WHEN OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR OTHER
PARTIES PROVIDE THE SOFTWARE "AS IS" WITHOUT WARRANTY OF ANY KIND,
EITHER EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE. THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THE
SOFTWARE IS WITH YOU. SHOULD THE SOFTWARE PROVE DEFECTIVE, YOU ASSUME
THE COST OF ALL NECESSARY SERVICING, REPAIR, OR CORRECTION.

IN NO EVENT UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING
WILL ANY COPYRIGHT HOLDER, OR ANY OTHER PARTY WHO MAY MODIFY AND/OR
REDISTRIBUTE THE SOFTWARE AS PERMITTED BY THE ABOVE LICENCE, BE LIABLE
TO YOU FOR DAMAGES, INCLUDING ANY GENERAL, SPECIAL, INCIDENTAL, OR
CONSEQUENTIAL DAMAGES ARISING OUT OF THE USE OR INABILITY TO USE THE
SOFTWARE (INCLUDING BUT NOT LIMITED TO LOSS OF DATA OR DATA BEING
RENDERED INACCURATE OR LOSSES SUSTAINED BY YOU OR THIRD PARTIES OR A
FAILURE OF THE SOFTWARE TO OPERATE WITH ANY OTHER SOFTWARE), EVEN IF
SUCH HOLDER OR OTHER PARTY HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH
DAMAGES.

=cut
