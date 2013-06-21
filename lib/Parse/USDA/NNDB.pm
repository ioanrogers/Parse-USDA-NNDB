package Parse::USDA::NNDB;

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
# XXX option to download old releases?
# TODO progress bars...

=attr C<base_dir>

This is ithe directory into which the data files will be downloaded.
Defaults to $HOME/.cache/usda_nndb

=cut

has base_dir => ( is => 'lazy' );
has _data_dir => ( is => 'lazy' );
has _src_uri  => ( is => 'lazy' );
has _src_file => ( is => 'lazy',);
has _fh => ( is => 'rw', predicate => 1, clearer => 1);
has _csv => (is => 'lazy', predicate => 1, clearer => 1);

=attr C<tables>

An arrayref of all the known tables.

=cut

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

=attr C<get_columns_for($table)>

Returns an arrayref of the column names used in this table, or undef if the
table is unknown

=cut

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

=method C<table ($table)>

Given a case-insenstive table name, this method sets the current active table
and opens the file ready for parsing. You must call this before B<get_line>.

Returns true on success, throws an exception on error.

=cut

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

=item C<get_line>

Returns the next line in the data file and returns a hashref
(see USDA docs for their meanings).

Returns undef when the file is finished or if something goes wrong.

=cut

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

=head1 METHODS

=over

=item C<new($basedir)>

Creates a new Parse::USDA::NNDB object. Takes one optional argument, a path
to the dir which will store the datafiles to be parsed.

=head1 SEE ALSO

L<USDA documentation|http://www.ars.usda.gov/Services/docs.htm?docid=8964>
