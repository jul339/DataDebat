"""
Module de transformation des débats de l'Assemblée Nationale
Extraction et parsing des fichiers TAZ/XML vers des documents structurés
"""

import os
import tarfile
import io
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import re


class ANDebatsTransformer:
    """Extracteur et transformateur de débats de l'Assemblée Nationale"""

    def extract_xml_from_taz(self, taz_path: str) -> Tuple[Optional[ET.Element], str]:
        """
        Extrait et parse le fichier CRI XML depuis un fichier TAZ
        La structure est: .taz contient .tar qui contient .xml

        Args:
            taz_path: Chemin vers le fichier .taz

        Returns:
            Tuple (root XML Element, nom du fichier XML) ou (None, "") si erreur
        """
        try:
            print(f"Ouverture de {os.path.basename(taz_path)}...")

            # Ouvrir le fichier .taz (qui contient un .tar)
            with tarfile.open(taz_path, "r:*") as taz:
                # Trouver le fichier .tar à l'intérieur
                membre_tar = None
                for m in taz.getmembers():
                    if m.name.endswith(".tar"):
                        membre_tar = m
                        break

                if not membre_tar:
                    print("⚠ Aucun fichier .tar trouvé dans le .taz")
                    return None, ""

                print(f"✓ Fichier TAR trouvé: {membre_tar.name}")

                # Extraire le .tar en mémoire
                tar_bytes = taz.extractfile(membre_tar).read()
                tar_buffer = io.BytesIO(tar_bytes)

                # Ouvrir le .tar depuis la mémoire
                with tarfile.open(fileobj=tar_buffer, mode="r:") as tar:
                    # Chercher le fichier CRI XML
                    for membre in tar.getmembers():
                        if membre.name.startswith("CRI_") and membre.name.endswith(
                            ".xml"
                        ):
                            print(f"✓ Fichier XML trouvé: {membre.name}")

                            # Extraire et parser le XML directement en mémoire
                            xml_file = tar.extractfile(membre)
                            xml_content = xml_file.read()

                            # Parser le XML
                            root = ET.fromstring(xml_content)

                            return root, membre.name

                    print("⚠ Aucun fichier CRI XML trouvé dans le TAR")
                    return None, ""

        except Exception as e:
            print(f"✗ Erreur lors de l'extraction: {e}")
            import traceback

            traceback.print_exc()
            return None, ""

    def parse_date(self, date_str: str) -> Optional[str]:
        """
        Parse les dates au format 'Mercredi-22-05-Mai-2013' vers 'YYYY-MM-DD'

        Args:
            date_str: Date au format de l'AN

        Returns:
            Date au format ISO
        """
        try:
            # Format: Jour-JJ-MM-Mois-AAAA
            parts = date_str.split("-")
            if len(parts) >= 3:
                jour = parts[1].zfill(2)
                mois = parts[2].zfill(2)
                annee = parts[-1]
                return f"{annee}-{mois}-{jour}"
        except:
            pass
        return None

    def extract_metadata(self, root: ET.Element) -> Dict:
        """
        Extrait les métadonnées du document XML

        Args:
            root: Élément racine du XML

        Returns:
            Dictionnaire des métadonnées
        """
        metadata = {}

        meta_elem = root.find(".//Metadonnees")
        if meta_elem is not None:
            # Publication
            pub_num = meta_elem.find("PublicationNumero")
            if pub_num is not None:
                metadata["publication_numero"] = int(pub_num.text)

            # Dates
            date_parution = meta_elem.find("DateParution")
            if date_parution is not None:
                metadata["date_parution"] = self.parse_date(date_parution.text)

            date_seance = meta_elem.find("DateSeance")
            if date_seance is not None:
                metadata["date_seance"] = self.parse_date(date_seance.text)
                # Extraire année et mois
                if metadata.get("date_seance"):
                    parts = metadata["date_seance"].split("-")
                    metadata["annee"] = int(parts[0])
                    metadata["mois"] = int(parts[1])

            # Session
            session_nom = meta_elem.find("SessionNom")
            if session_nom is not None:
                metadata["session_nom"] = session_nom.text

            session_parl = meta_elem.find("SessionParlementaire")
            if session_parl is not None:
                metadata["session_parlementaire"] = session_parl.text

            # Législature
            legislature = meta_elem.find("LegislatureNumero")
            if legislature is not None:
                metadata["legislature"] = int(legislature.text)

            # Numéro de première page
            premiere_page = meta_elem.find("NumeroPremierePage")
            if premiere_page is not None:
                metadata["numero_premiere_page"] = int(premiere_page.text)

        return metadata

    def clean_text(self, text: str) -> str:
        """Nettoie le texte en supprimant les espaces multiples et caractères parasites"""
        if not text:
            return ""
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def remove_speaker_prefix(self, text: str, orateur_nom: Optional[str]) -> str:
        """
        Supprime le nom de l'orateur au début du texte

        Args:
            text: Texte brut extrait
            orateur_nom: Nom de l'orateur (ex: "M. Thomas Mesnier")

        Returns:
            Texte nettoyé sans le préfixe de l'orateur
        """
        if not text:
            return ""

        # Supprimer le nom de l'orateur s'il est au début du texte
        if orateur_nom:
            # Pattern pour matcher le nom de l'orateur suivi d'un point ou de ponctuation
            pattern = rf"^{re.escape(orateur_nom)}\.?\s*"
            text = re.sub(pattern, "", text, flags=re.IGNORECASE)

        # Supprimer aussi les patterns génériques d'orateur au début
        # Ex: "M. le président.", "Mme la ministre.", etc.
        text = re.sub(r"^(M\.|Mme|Mlle)\.?\s+[^.]+\.\s*", "", text)

        return text.strip()

    def extract_orateur(self, para_elem: ET.Element) -> Dict:
        """
        Extrait les informations sur l'orateur

        Args:
            para_elem: Élément Para contenant l'intervention

        Returns:
            Dictionnaire avec nom et fonction de l'orateur
        """
        orateur_info = {}

        orateur_elem = para_elem.find(".//Orateur")
        if orateur_elem is not None:
            nom_elem = orateur_elem.find("Nom")
            if nom_elem is not None:
                orateur_info["nom"] = self.clean_text(nom_elem.text)

            # Fonction peut être déduite du nom (ex: "M. le président.")
            if orateur_info.get("nom"):
                nom_lower = orateur_info["nom"].lower()
                if "président" in nom_lower:
                    orateur_info["fonction"] = "Président"
                elif "ministre" in nom_lower:
                    orateur_info["fonction"] = "Ministre"
                elif "secrétaire" in nom_lower:
                    orateur_info["fonction"] = "Secrétaire"
                else:
                    orateur_info["fonction"] = "Député"

        return orateur_info

    def extract_vote(self, section_elem: ET.Element) -> Optional[Dict]:
        """
        Extrait les résultats de vote s'ils sont présents

        Args:
            section_elem: Élément Section contenant potentiellement un vote

        Returns:
            Dictionnaire avec les résultats du vote ou None
        """
        vote_elem = section_elem.find(".//ResultatVote")
        if vote_elem is None:
            return None

        vote_data = {"vote_present": True}

        # Nombre de votants
        votants = vote_elem.find(".//NombreVotants/Valeur")
        if votants is not None:
            vote_data["nombre_votants"] = int(votants.text)

        # Suffrages exprimés
        suffrages = vote_elem.find(".//NombreSuffrageExprime/Valeur")
        if suffrages is not None:
            vote_data["nombre_suffrages_exprimes"] = int(suffrages.text)

        # Pour
        pour = vote_elem.find(".//Pour/Valeur")
        if pour is not None:
            vote_data["votes_pour"] = int(pour.text)

        # Contre
        contre = vote_elem.find(".//Contre/Valeur")
        if contre is not None:
            vote_data["votes_contre"] = int(contre.text)

        return vote_data

    def extract_text_recursive(self, elem: ET.Element) -> str:
        """Extrait récursivement tout le texte d'un élément et ses enfants"""
        texts = []
        if elem.text:
            texts.append(elem.text)
        for child in elem:
            texts.append(self.extract_text_recursive(child))
            if child.tail:
                texts.append(child.tail)
        return " ".join(texts)

    def save_documents_to_file(
        self,
        documents: List[Dict],
        output_file: str = "documents_output.json",
        save_transform_file: bool = False,
    ):
        """
        Sauvegarde les documents extraits dans un fichier JSON

        Args:
            documents: Liste des documents à sauvegarder
            output_file: Chemin du fichier de sortie
        """
        # Extraire seulement orateur_nom et texte
        filtered_docs = [
            {
                "fonction": doc.get("orateur_fonction", "N/A"),
                "para_id": doc.get("para_id", ""),
                "orateur_nom": doc.get("orateur_nom", "N/A"),
                "texte": doc.get("texte", ""),
                "section_titre": doc.get("section_titre", "N/A"),
                "sous_section_titre": doc.get("sous_section_titre", "N/A"),
                "section_id": doc.get("section_id", "N/A"),
            }
            for doc in documents
        ]

        # Créer le répertoire si nécessaire
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # Charger les documents existants si le fichier existe déjà
        existing_docs = []
        print(f"save_transform_file: {save_transform_file}")
        if os.path.exists(output_file) and save_transform_file:
            try:
                with open(output_file, "r", encoding="utf-8") as f:
                    existing_docs = json.load(f)
                if not isinstance(existing_docs, list):
                    existing_docs = []
            except (json.JSONDecodeError, IOError) as e:
                print(
                    f"⚠ Avertissement: Impossible de lire le fichier existant {output_file}: {e}"
                )
                existing_docs = []

        # Fusionner les documents existants avec les nouveaux
        if save_transform_file:
            all_docs = existing_docs + filtered_docs
        else:
            all_docs = filtered_docs

        # Sauvegarder en JSON
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_docs, f, indent=2, ensure_ascii=False)

        print(
            f"✓ {len(filtered_docs)} nouveaux documents ajoutés ({len(all_docs)} documents au total) dans {output_file}"
        )

    def extract_sections(self, root: ET.Element, metadata: Dict) -> List[Dict]:
        """
        Extrait toutes les sections et interventions du débat

        Args:
            root: Élément racine du XML
            metadata: Métadonnées du document

        Returns:
            Liste de documents à indexer
        """
        documents = []

        # Chercher toutes les sections
        for section in root.findall(".//Section"):
            section_data = metadata.copy()

            # Titre de section
            titre_struct = section.find("./TitreStruct")
            if titre_struct is not None:
                section_id = titre_struct.get("Ident", "")
                section_data["section_id"] = section_id

                intitule = titre_struct.find(".//Intitule")
                if intitule is not None:
                    section_data["section_titre"] = self.clean_text(
                        self.extract_text_recursive(intitule)
                    )

            # Vérifier s'il y a des sous-sections
            sous_sections_1 = section.findall("./SousSection1")
            sous_sections_2 = section.findall("./SousSection2")
            sous_sections = sous_sections_1 + sous_sections_2

            if sous_sections:
                # Traiter chaque sous-section séparément
                for sous_section in sous_sections:
                    sous_section_data = section_data.copy()

                    # Extraire le titre de la sous-section
                    ss_titre_struct = sous_section.find("./TitreStruct")
                    if ss_titre_struct is not None:
                        ss_intitule = ss_titre_struct.find(".//Intitule")
                        if ss_intitule is not None:
                            sous_section_data["sous_section_titre"] = self.clean_text(
                                self.extract_text_recursive(ss_intitule)
                            )
                    # Extraire les paragraphes de cette sous-section
                    documents.extend(
                        self._extract_paragraphs(sous_section, sous_section_data)
                    )
            else:
                # Pas de sous-sections, traiter les paragraphes directement
                documents.extend(self._extract_paragraphs(section, section_data))

            print(
                f"Extracted {len(documents)} paragraphs from section {section_data.get('section_id', '')}"
            )

        return documents

    def _extract_paragraphs(
        self, parent_elem: ET.Element, base_data: Dict
    ) -> List[Dict]:
        """
        Extrait les paragraphes d'un élément parent (Section ou SousSection2)

        Args:
            parent_elem: Élément parent contenant les paragraphes
            base_data: Données de base à copier pour chaque paragraphe

        Returns:
            Liste de documents extraits
        """
        documents = []
        last_para_data = None

        # Extraire les paragraphes directs (pas ceux dans des sous-éléments imbriqués)
        for para in parent_elem.findall("./Para"):
            para_id = para.get("idsyceron")

            # Ignorer les paragraphes sans identifiant
            if para_id is None:
                continue

            # Cas 1: Continuation du paragraphe précédent (même id)
            if last_para_data is not None and para_id == last_para_data["para_id"]:
                raw_text = self.extract_text_recursive(para)
                cleaned_text = self.remove_speaker_prefix(
                    raw_text, last_para_data.get("orateur_nom")
                )
                last_para_data["texte"] += " " + cleaned_text
                continue

            # Cas 2: Nouveau paragraphe
            para_data = base_data.copy()
            para_data["para_id"] = para_id
            # Extraction de l'orateur
            orateur_info = self.extract_orateur(para)
            para_data["orateur_nom"] = orateur_info.get("nom")
            para_data["orateur_fonction"] = orateur_info.get("fonction")
            para_data["extraction_timestamp"] = datetime.now().isoformat()
            para_data["vote_present"] = False

            # Extraire le texte et supprimer le préfixe de l'orateur
            raw_text = self.extract_text_recursive(para)
            para_data["texte"] = self.remove_speaker_prefix(
                raw_text, para_data["orateur_nom"]
            )

            documents.append(para_data)
            last_para_data = para_data

        return documents

    def process_taz_file(
        self,
        taz_path: str,
        output_dir: str = "./data/transformed",
        save_transform_file: bool = False,
    ) -> List[Dict]:
        """
        Traite un fichier TAZ complet: extraction et parsing

        Args:
            taz_path: Chemin vers le fichier .taz
            output_dir: Répertoire de sortie pour les fichiers JSON
            save_transform_file: Sauvegarder les informations de transformation
        Returns:
            Liste des documents extraits
        """

        try:
            # Étape 1: Extraire et parser le XML directement depuis le .taz
            root, xml_filename = self.extract_xml_from_taz(taz_path)

            if root is None:
                print("✗ Impossible d'extraire le XML")
                return []

            # Étape 2: Extraire les métadonnées
            metadata = self.extract_metadata(root)

            # Étape 3: Extraire les sections et interventions
            documents = self.extract_sections(root, metadata)

            # Étape 4: Sauvegarder en JSON

            year = metadata.get("annee", "unknown")
            output_file = (
                f"{output_dir}/{year}/{metadata.get('date_seance', 'N/A')}.json"
            )
            self.save_documents_to_file(documents, output_file, save_transform_file)

            print(f"✓ {len(documents)} interventions extraites")

            return documents

        except Exception as e:
            print(f"✗ Erreur lors du traitement: {e}")
            import traceback

            traceback.print_exc()
            return []

    def process_directory(
        self,
        directory: str,
        output_dir: str = "./data/transformed",
        save_transform_file: bool = False,
    ) -> List[Dict]:
        """
        Traite tous les fichiers TAZ d'un répertoire

        Args:
            directory: Chemin vers le répertoire contenant les fichiers TAZ
            output_dir: Répertoire de sortie pour les fichiers JSON
            save_transform_file: Sauvegarder les informations de transformation
        Returns:
            Liste de tous les documents extraits
        """
        taz_files = list(Path(directory).glob("*.taz"))

        if not taz_files:
            print(f"⚠ Aucun fichier .taz trouvé dans {directory}")
            return []

        print(f"{'='*60}")
        print(f"Traitement de {len(taz_files)} fichier(s) TAZ")

        all_documents = []
        for i, taz_file in enumerate(taz_files, 1):
            print(f"\n[{i}/{len(taz_files)}]")
            documents = self.process_taz_file(
                str(taz_file), output_dir, save_transform_file
            )
            all_documents.extend(documents)

        print(f"Traitement global terminé: {len(all_documents)} documents extraits")
        print(f"{'='*60}")

        return all_documents


if __name__ == "__main__":
    transformer = ANDebatsTransformer()
    transformer.process_taz_file("./data/raw/2022/AN_2022002.taz")
