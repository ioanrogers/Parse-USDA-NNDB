package Parse::USDA::NNDB;

# ABSTRACT: download and parse the latest USDA nutritional information database

use v5.10.0;
use strict;
use warnings;

use Text::CSV_XS;
use Archive::Zip qw/ :ERROR_CODES :CONSTANTS /;
use File::HomeDir;
use File::Spec;
use URI;
use File::Fetch;
use Log::Any;

# TODO use the updates rather than a whole new db

sub new {
    my ( $this, $base_dir ) = @_;
    my $class = ref( $this ) || $this;

    # TODO better cross-platform defaults
    if ( !defined $base_dir ) {
        $base_dir = File::Spec->catdir( File::HomeDir->my_home, '.cache/usda_nndb' );
    }

    # TODO set up base dir
    my $self = {
        base_dir => $base_dir,
        data_dir => File::Spec->catdir( $base_dir, 'sr24' ),
        data_uri => URI->new( 'http://www.ars.usda.gov/SP2UserFiles/Place/12354500/Data/SR24/dnload/sr24.ZIP' ),
        zip_file => File::Spec->catfile( $base_dir, 'sr24.ZIP' ),
        logger   => Log::Any->get_logger( category => __PACKAGE__ ),
    };

    bless $self, $class;
    return $self;
}

sub _get_file_path_for {
    my ( $self, $table ) = @_;

    my $file_path = File::Spec->catfile( $self->{data_dir}, $table . ".txt" );
    $self->{logger}->debug( "Using path [$file_path] for '$table'" );
    return $file_path;
}

sub tables {
    return qw/DATSRCLN FD_GROUP FOOTNOTE NUTR_DEF SRC_CD DATA_SRC DERIV_CD FOOD_DES NUT_DATA WEIGHT/;
}

sub get_columns_for {
    my ( $self, $table ) = @_;

    given ( $table ) {
        when ( /^FOOD_DES$/i ) {
            return [
                qw/NDB_No FdGrp_Cd Long_Desc Shrt_Desc ComName ManufacName Survey Ref_desc Refuse SciName N_Factor Pro_Factor Fat_Factor CHO_Factor/
              ],
              [qw/0 1/]
        }
        when ( /^FD_GROUP$/i ) {
            return [qw/FdGrp_Cd FdGrp_Desc/]
        }
        when ( /^NUT_DATA$/i ) {
            return ( [
                    qw/NDB_No Nutr_No Nutr_Val Num_Data_Pts Std_Error Src_Cd Deriv_Cd Ref_NDB_No Add_Nutr_Mark Num_Studies Min Max DF Low_EB Up_EB Stat_cmt CC/
                ],
                [qw/0 1/] )
        }
        when ( /^NUTR_DEF$/i ) {
            return ( [qw(Nutr_No Units Tagname NutrDesc Num_Desc SR_Order)], qw/0/ )
        }
        when ( /^SRC_CD$/i ) {
            return 0, [qw(Src_Cd SrcCd_Desc)]
        }
        when ( /^DERIV_CD$/i ) {
            return 0, [qw(Deriv_Cd Deriv_Desc)]
        }
        when ( /^WEIGHT$/i ) {
            return 0, [qw(NDB_No Seq Amount Msre_Desc Gm_Wgt Num_Data_Pts Std_Dev)]
        }
        when ( /^FOOTNOTE$/i ) {
            return 0, [qw(NDB_No Footnt_No Footnt_Typ Nutr_No Footnt_Txt)]
        }
        when ( /^DATSRCLN$/i ) {
            return 0, [qw(NDB_No Nutr_No DataSrc_ID)]
        }
        when ( /^DATA_SRC$/i ) {
            return 0, [qw(DataSrc_ID Authors Title Year Journal Vol_City Issue_State Start_Page End_Page)]
        }
        when ( /^ABBREV$/i ) {
            return [
                qw(NDB_No Shrt_Desc Water Energ_Kcal Protein Lipit_Tot Ash Carbonhydrt Fiber_TD Sugar_Tot Calcium Iron Magnesium Phosphorus Potassium Sodium Zinc Copper Manganese Selenium Vit_C Thiamin Riboflavin Niacin Panto_acid Vit_B6 Folate_Tot Folic_acid Food_Folate Folate_DFE Choline_total Vit_B12 Vit_A_IU Vit_A_RAE Retinol Alpha_Carot Beta_Carot Beta_Crypt Lycopene Lut_and_Zea Vit_E Vit_K FA_Sat FA_Mono FA_Poly Cholestrl GmWt_1 GmWt_Desc1 GmWt_2 GmWt_Desc2 Refuse_Pct)
              ]
        }
        default {
            warn "Unknown table '$table' requested\n";
            return 0;
        }
    }
}

sub parse_file {
    my ( $self, $table ) = @_;

    my $csv = Text::CSV_XS->new( {
            quote_char          => '~',
            sep_char            => '^',
            allow_loose_escapes => 1,
            auto_diag           => 1,
            empty_is_undef      => 1,
    } );
    my $file_path = $self->_get_file_path_for( $table );

    if ( !-e $file_path ) {
        $self->_fetch_data
          or return 0;
    }

    open my $fh, '<', $file_path;

    # TODO better error handling!
    if ( !$fh ) {
        my $err = $self->{logger}->crit( "Could not open [$file_path]: $!" );
        die;
    }

    my ( $column_names ) = $self->get_columns_for( $table )
      or return 0;

    $csv->column_names( $column_names );

    my @rows;
    while ( my $row = $csv->getline_hr( $fh ) ) {

        push @rows, $row;
    }

    $csv->eof;

    my ( $code, $str, $pos ) = $csv->error_diag;
    if ( $str ) {
        $self->{logger}->critf( "CSV parse error at %s: %s", $pos, $str );
    }

    close $fh;
    return \@rows;
}

sub _fetch_data {
    my $self = shift;

    # does zip already exist?
    if ( -e $self->{zip_file} ) {
        if ( !$self->_extract_data ) {

            # failed to extract, file is corrupt? User should try again
            unlink $self->{zip_file}
              or die sprintf "Failed to remove cached file '%s': %s\n", $self->{zip_file}, $!;
        } else {
            return 1;
        }

    }

    my $ff = File::Fetch->new( uri => $self->{data_uri} );

    say "Downloading " . $self->{data_uri} . " to " . $self->{base_dir};
    my $file = $ff->fetch( to => $self->{base_dir} )
      or warn $ff->error;

    $self->{zip_file} = $file;    # should have been the same anyway
    say "Saved data to $file";

    $self->_extract_data;

    return 1;
}

sub _extract_data {
    my $self = shift;

    my $zip = Archive::Zip->new;

    unless ( $zip->read( $self->{zip_file} ) == AZ_OK ) {
        warn "Read error";
        return 0;
    }

    $zip->extractTree( undef, $self->{data_dir} . "/" );

    return 1;
}

1;

__END__

=head1 SEE ALSO

L<USDA National Nutrient Database for Standard Reference|http://www.ars.usda.gov/Services/docs.htm?docid=8964>