from modfilegen.converter import Converter
from .soil_parameter_calculators import calculate_q0
import traceback

class SticsParamSolConverter(Converter):
    def __init__(self):
        super().__init__()

    def export(
        self, parameter_resolver, soil_repository, usmdir, id_soil,
        q0_strategy="default",
    ):
        file_name = "param.sol"
        defaults = parameter_resolver.resolve("sticsv11", "paramsol", id_soil)
        row = soil_repository.get_soil(id_soil)
        layers = soil_repository.get_layers(id_soil)
        if parameter_resolver.has_override(
            "sticsv11", "paramsol", "q0", id_soil
        ):
            print(f"Using overridden q0 value for soil ID {id_soil}")
            q0 = defaults["q0"]
        elif str(q0_strategy).strip().casefold() == "computed":
            q0 = calculate_q0(row)
        else:
            q0 = defaults["q0"]        
        file_lines = []
        if row:
            line1 = [
                "     1  ","Sol", f"{row['Clay']:.1f}", f"{row['OrganicNStock']:.4f}",
                f"{float(defaults['profhum']):.4f}", f"{float(defaults['calc']):.4f}",
                f"{row['pH']:.4f}", f"{float(defaults['concseuil']):.4f}",
                f"{row['albedo']:.4f}", f"{float(q0):.4f}",
                f"{row['RunoffCoefBSoil']:.4f}", f"{row['SoilRDepth']:.4f}",
                f"{float(defaults['pluiebat']):.4f}", f"{float(defaults['mulchbat']):.4f}",
                f"{float(defaults['zesx']):.4f}", f"{float(defaults['cfes']):.4f}",
                f"{float(defaults['z0solnu']):.4f}", f"{row['OrganicC']/row['OrganicNStock']:.4f}",
                f"{float(defaults['finert']):.5f}", f"{float(defaults['penterui']):.4f}"
            ]
            file_lines.append(" ".join(line1))

            codes = ["codecailloux", "codemacropor", "codefente", "codrainage", "coderemontcap", "codenitrif", "codedenit"]
            line2 = ["     1  "] + [f"{int(float(defaults[c])):.0f}" for c in codes]
            file_lines.append(" ".join(line2))             
            
            line3 = [
                "     1  ",
                f"{float(defaults['profimper']):.4f}", f"{float(defaults['ecartdrain']):.4f}",
                f"{float(defaults['ksol']):.4f}", f"{float(defaults['profdrain']):.4f}",
                f"{float(defaults['capiljour']):.4f}", f"{float(defaults['humcapil']):.4f}",
                f"{int(float(defaults['profdenit'])):.0f}", f"{float(defaults['vpotdenit']):.4f}"
            ]
            file_lines.append(" ".join(line3))            
            for i in range(5):
                if row["SoilOption"] == "simple":
                    #fileContent += "     1   "
                    file_lines.append("     1  ")
                    if i == 0:
                        #fileContent += format(row["SoilTotalDepth"], ".2f") + " "
                        depth = f"{row['SoilTotalDepth']:.2f} "
                    else:
                        #fileContent += "0.00 "  
                        depth = "0.00" 
                    values = [
                        depth,
                        f"{row['Wfc']/row['bd']:.2f}",
                        f"{row['Wwp']/row['bd']:.2f}",
                        f"{row['bd']:.2f}",
                        f"{row['cf']:.2f}",
                        f"{int(defaults['typecailloux'])}",
                        f"{int(float(defaults['infil']))}",
                        f"{int(defaults['epd'])}"
                    ]
                    file_lines[-1] += " " + " ".join(values)
                else:
                    if i < len(layers):
                        values = ["     1  ",
                            f"{layers[i]['Ldown'] - layers[i]['Lup']:.2f}",
                            f"{layers[i]['Wfc']/layers[i]['bd']:.2f}",
                            f"{layers[i]['Wwp']/layers[i]['bd']:.2f}",
                            f"{layers[i]['bd']:.2f}",
                            f"{row['cf']:.2f}",
                            f"{int(defaults['typecailloux'])}",
                            f"{int(float(defaults['infil']))}",
                            f"{int(defaults['epd'])}"
                        ]
                        
                    else:
                        values = ["     1  ","0.00","0.00","0.00","0.00",f"{row['cf']:.2f}",
                            f"{int(defaults['typecailloux'])}",
                            f"{int(float(defaults['infil']))}",
                            f"{int(defaults['epd'])}"
                        ]
                    file_lines.append(" ".join(values))
        try:
            #self.write_file(usmdir, file_name, fileContent) 
            self.write_file(usmdir, file_name, "\n".join(file_lines))
        except Exception as e:
            traceback.print_exc()
            print(f"Error during writing file : {e}")
        return   "\n".join(file_lines)      #fileContent
            

