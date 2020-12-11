use super::SqlSchemaDifferFlavour;
use crate::sql_schema_differ::column::ColumnDiffer;
use crate::sql_schema_differ::column::ColumnTypeChange;
use crate::{flavour::MssqlFlavour, sql_schema_differ::SqlSchemaDiffer};
use native_types::{MsSqlType, MsSqlTypeParameter};
use sql_schema_describer::walkers::IndexWalker;
use sql_schema_describer::ColumnTypeFamily;
use std::collections::HashSet;

impl SqlSchemaDifferFlavour for MssqlFlavour {
    fn should_skip_index_for_new_table(&self, index: &IndexWalker<'_>) -> bool {
        index.index_type().is_unique()
    }

    fn should_recreate_the_primary_key_on_column_recreate(&self) -> bool {
        true
    }

    fn tables_to_redefine(&self, differ: &SqlSchemaDiffer<'_>) -> HashSet<String> {
        let autoincrement_changed = differ
            .table_pairs()
            .filter(|differ| differ.column_pairs().any(|c| c.autoincrement_changed()))
            .map(|table| table.next().name().to_owned());

        let all_columns_of_the_table_gets_dropped = differ
            .table_pairs()
            .filter(|tables| {
                tables.column_pairs().all(|columns| {
                    let type_changed = columns.previous.column_type_family() != columns.next.column_type_family();
                    let not_castable = matches!(type_change_riskyness(&columns), ColumnTypeChange::NotCastable);

                    type_changed && not_castable
                })
            })
            .map(|tables| tables.previous().name().to_string());

        autoincrement_changed
            .chain(all_columns_of_the_table_gets_dropped)
            .collect()
    }

    fn column_type_change(&self, differ: &ColumnDiffer<'_>) -> Option<ColumnTypeChange> {
        if differ.previous.column_type_family() == differ.next.column_type_family() {
            None
        } else {
            Some(type_change_riskyness(differ))
        }
    }
}

fn type_change_riskyness(differ: &ColumnDiffer<'_>) -> ColumnTypeChange {
    match (differ.previous.column_type_family(), differ.next.column_type_family()) {
        (_, ColumnTypeFamily::String) => ColumnTypeChange::SafeCast,
        (ColumnTypeFamily::String, ColumnTypeFamily::Int)
        | (ColumnTypeFamily::DateTime, ColumnTypeFamily::Float)
        | (ColumnTypeFamily::String, ColumnTypeFamily::Float) => ColumnTypeChange::NotCastable,
        (_, _) => ColumnTypeChange::RiskyCast,
    }
}

fn native_type_change_riskyness(differ: &ColumnDiffer<'_>) -> ColumnTypeChange {
    use MsSqlTypeParameter::*;

    let (previous_type, next_type): (Option<MsSqlType>, Option<MsSqlType>) =
        (differ.previous.column_native_type(), differ.next.column_native_type());

    match (previous_type, next_type) {
        (None, _) | (_, None) => type_change_riskyness(differ),
        (left, right) if left == right => ColumnTypeChange::SafeCast,

        (Some(MsSqlType::Bit), Some(MsSqlType::TinyInt)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Bit), Some(MsSqlType::SmallInt)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Bit), Some(MsSqlType::Int)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Bit), Some(MsSqlType::BigInt)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Bit), Some(MsSqlType::Decimal(_))) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Bit), Some(MsSqlType::Numeric(_))) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Bit), Some(MsSqlType::Money)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Bit), Some(MsSqlType::SmallMoney)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Bit), Some(MsSqlType::Float(_))) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Bit), Some(MsSqlType::Real)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Bit), Some(MsSqlType::DateTime)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Bit), Some(MsSqlType::SmallDateTime)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Bit), Some(MsSqlType::Binary(_))) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Bit), Some(MsSqlType::VarBinary(_))) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Bit), Some(MsSqlType::Text)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Bit), Some(MsSqlType::NText)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Bit), Some(MsSqlType::Image)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Bit), Some(MsSqlType::Xml)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Bit), Some(MsSqlType::UniqueIdentifier)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Bit), Some(MsSqlType::Date)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Bit), Some(MsSqlType::Time)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Bit), Some(MsSqlType::DateTime2)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Bit), Some(MsSqlType::DateTimeOffset)) => ColumnTypeChange::NotCastable,

        (Some(MsSqlType::TinyInt), Some(MsSqlType::SmallInt)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::Int)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::BigInt)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::Decimal(_))) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::Numeric(_))) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::Money)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::SmallMoney)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::Float(_))) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::Real)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::DateTime)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::SmallDateTime)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::Binary(_))) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::VarBinary(_))) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::Text)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::NText)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::Image)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::Xml)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::UniqueIdentifier)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::Date)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::Time)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::DateTime2)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::TinyInt), Some(MsSqlType::DateTimeOffset)) => ColumnTypeChange::NotCastable,

        (Some(MsSqlType::SmallInt), Some(MsSqlType::Int)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::SmallInt), Some(MsSqlType::BigInt)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::SmallInt), Some(MsSqlType::Decimal(params))) => match params {
            Some((p, s)) if p - s < 5 => ColumnTypeChange::RiskyCast,
            _ => ColumnTypeChange::SafeCast,
        },
        (Some(MsSqlType::SmallInt), Some(MsSqlType::Numeric(params))) => match params {
            Some((p, s)) if p - s < 5 => ColumnTypeChange::RiskyCast,
            _ => ColumnTypeChange::SafeCast,
        },
        (Some(MsSqlType::SmallInt), Some(MsSqlType::Money)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::SmallInt), Some(MsSqlType::SmallMoney)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::SmallInt), Some(MsSqlType::Float(_))) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::SmallInt), Some(MsSqlType::Real)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::SmallInt), Some(MsSqlType::DateTime)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::SmallInt), Some(MsSqlType::SmallDateTime)) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::SmallInt), Some(MsSqlType::Binary(param))) => match param {
            Some(n) if n < 2 => ColumnTypeChange::RiskyCast,
            None => ColumnTypeChange::RiskyCast, // n == 1 by default
            _ => ColumnTypeChange::SafeCast,
        },
        (Some(MsSqlType::SmallInt), Some(MsSqlType::VarBinary(param))) => match param {
            Some(Number(n)) if n < 2 => ColumnTypeChange::RiskyCast,
            None => ColumnTypeChange::RiskyCast, // n == 1 by default
            _ => ColumnTypeChange::SafeCast,
        },
        (Some(MsSqlType::SmallInt), Some(MsSqlType::Text)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::SmallInt), Some(MsSqlType::NText)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::SmallInt), Some(MsSqlType::Image)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::SmallInt), Some(MsSqlType::Xml)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::SmallInt), Some(MsSqlType::UniqueIdentifier)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::SmallInt), Some(MsSqlType::Date)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::SmallInt), Some(MsSqlType::Time)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::SmallInt), Some(MsSqlType::DateTime2)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::SmallInt), Some(MsSqlType::DateTimeOffset)) => ColumnTypeChange::NotCastable,

        (Some(MsSqlType::Int), Some(MsSqlType::BigInt)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Int), Some(MsSqlType::Decimal(params))) => match params {
            Some((p, s)) if p - s < 10 => ColumnTypeChange::RiskyCast,
            _ => ColumnTypeChange::SafeCast,
        },
        (Some(MsSqlType::Int), Some(MsSqlType::Numeric(params))) => match params {
            Some((p, s)) if p - s < 10 => ColumnTypeChange::RiskyCast,
            _ => ColumnTypeChange::SafeCast,
        },
        (Some(MsSqlType::Int), Some(MsSqlType::Money)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Int), Some(MsSqlType::SmallMoney)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Int), Some(MsSqlType::Float(_))) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Int), Some(MsSqlType::Real)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Int), Some(MsSqlType::DateTime)) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Int), Some(MsSqlType::SmallDateTime)) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::Int), Some(MsSqlType::Binary(param))) => match param {
            Some(n) if n < 4 => ColumnTypeChange::RiskyCast,
            None => ColumnTypeChange::RiskyCast, // n == 1 by default
            _ => ColumnTypeChange::SafeCast,
        },
        (Some(MsSqlType::Int), Some(MsSqlType::VarBinary(param))) => match param {
            Some(Number(n)) if n < 4 => ColumnTypeChange::RiskyCast,
            None => ColumnTypeChange::RiskyCast, // n == 1 by default
            _ => ColumnTypeChange::SafeCast,
        },
        (Some(MsSqlType::Int), Some(MsSqlType::Text)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Int), Some(MsSqlType::NText)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Int), Some(MsSqlType::Image)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Int), Some(MsSqlType::Xml)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Int), Some(MsSqlType::UniqueIdentifier)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Int), Some(MsSqlType::Date)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Int), Some(MsSqlType::Time)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Int), Some(MsSqlType::DateTime2)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Int), Some(MsSqlType::DateTimeOffset)) => ColumnTypeChange::NotCastable,

        (Some(MsSqlType::BigInt), Some(MsSqlType::Decimal(params))) => match params {
            Some((p, s)) if p - s < 19 => ColumnTypeChange::RiskyCast,
            None => ColumnTypeChange::RiskyCast, // p == 18, s == 0 by default
            _ => ColumnTypeChange::SafeCast,
        },
        (Some(MsSqlType::BigInt), Some(MsSqlType::Numeric(params))) => match params {
            Some((p, s)) if p - s < 19 => ColumnTypeChange::RiskyCast,
            None => ColumnTypeChange::RiskyCast, // p == 18, s == 0 by default
            _ => ColumnTypeChange::SafeCast,
        },
        (Some(MsSqlType::BigInt), Some(MsSqlType::Money)) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::BigInt), Some(MsSqlType::SmallMoney)) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::BigInt), Some(MsSqlType::Float(_))) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::BigInt), Some(MsSqlType::Real)) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::BigInt), Some(MsSqlType::DateTime)) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::BigInt), Some(MsSqlType::SmallDateTime)) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::BigInt), Some(MsSqlType::Binary(param))) => match param {
            Some(n) if n < 8 => ColumnTypeChange::RiskyCast,
            None => ColumnTypeChange::RiskyCast, // n == 1 by default
            _ => ColumnTypeChange::SafeCast,
        },
        (Some(MsSqlType::BigInt), Some(MsSqlType::VarBinary(param))) => match param {
            Some(Number(n)) if n < 8 => ColumnTypeChange::RiskyCast,
            None => ColumnTypeChange::RiskyCast, // n == 1 by default
            _ => ColumnTypeChange::SafeCast,
        },
        (Some(MsSqlType::BigInt), Some(MsSqlType::Text)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::BigInt), Some(MsSqlType::NText)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::BigInt), Some(MsSqlType::Image)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::BigInt), Some(MsSqlType::Xml)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::BigInt), Some(MsSqlType::UniqueIdentifier)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::BigInt), Some(MsSqlType::Date)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::BigInt), Some(MsSqlType::Time)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::BigInt), Some(MsSqlType::DateTime2)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::BigInt), Some(MsSqlType::DateTimeOffset)) => ColumnTypeChange::NotCastable,

        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::TinyInt)) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::SmallInt)) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::Int)) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::BigInt)) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::Numeric(_))) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::Money)) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::SmallMoney)) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::Bit)) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::Float(_))) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::Real)) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::Date)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::Time)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::DateTime)) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::SmallDateTime)) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::Text)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::NText)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::Image)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::Xml)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::UniqueIdentifier)) => ColumnTypeChange::NotCastable,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::Binary(_))) => ColumnTypeChange::RiskyCast,
        (Some(MsSqlType::Decimal(_)), Some(MsSqlType::VarBinary(_))) => ColumnTypeChange::RiskyCast,

        (Some(MsSqlType::Text), Some(MsSqlType::VarChar(Some(Max)))) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::Text), Some(MsSqlType::NVarChar(Some(Max)))) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::NText), Some(MsSqlType::NVarChar(Some(Max)))) => ColumnTypeChange::SafeCast,
        (Some(MsSqlType::NText), Some(MsSqlType::VarChar(Some(Max)))) => ColumnTypeChange::RiskyCast,

        (Some(_), Some(MsSqlType::Char(_))) => ColumnTypeChange::SafeCast,
        (Some(_), Some(MsSqlType::NChar(_))) => ColumnTypeChange::SafeCast,
        (Some(_), Some(MsSqlType::VarChar(_))) => ColumnTypeChange::SafeCast,
        (Some(_), Some(MsSqlType::NVarChar(_))) => ColumnTypeChange::SafeCast,
        //(Some(_), Some(MsSqlType::Text)) => ColumnTypeChange::SafeCast,
        //(Some(_), Some(MsSqlType::NText)) => ColumnTypeChange::SafeCast,
    }
}
